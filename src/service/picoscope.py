import asyncio
import logging
from collections.abc import AsyncIterator

import grpc
from google.protobuf.timestamp_pb2 import Timestamp
from h2pcontrol.picoscope.v1.picoscope_pb2 import (
    COUPLING_AC,
    COUPLING_DC,
    RESOLUTION_8_BIT,
    RESOLUTION_12_BIT,
    RESOLUTION_14_BIT,
    RESOLUTION_15_BIT,
    RESOLUTION_16_BIT,
    TRIGGER_DIRECTION_ABOVE,
    TRIGGER_DIRECTION_BELOW,
    TRIGGER_DIRECTION_FALLING,
    TRIGGER_DIRECTION_RISING,
    TRIGGER_DIRECTION_RISING_OR_FALLING,
    VOLTAGE_RANGE_1_V,
    VOLTAGE_RANGE_2_V,
    VOLTAGE_RANGE_5_V,
    VOLTAGE_RANGE_10_MV,
    VOLTAGE_RANGE_10_V,
    VOLTAGE_RANGE_20_MV,
    VOLTAGE_RANGE_20_V,
    VOLTAGE_RANGE_50_MV,
    VOLTAGE_RANGE_100_MV,
    VOLTAGE_RANGE_200_MV,
    VOLTAGE_RANGE_500_MV,
    ArmCaptureRequest,
    ArmCaptureResponse,
    ChannelTrace,
    ConfigureChannelRequest,
    ConfigureChannelResponse,
    ConfigureResolutionRequest,
    ConfigureResolutionResponse,
    ConfigureTimebaseRequest,
    ConfigureTimebaseResponse,
    ConfigureTriggerRequest,
    ConfigureTriggerResponse,
    Coupling,
    GetTimebasesRequest,
    GetTimebasesResponse,
    Resolution,
    StreamCapturesRequest,
    StreamCapturesResponse,
    TimebaseInfo,
    TriggerDirection,
    VoltageRange,
)
from h2pcontrol.picoscope.v1.picoscope_pb2_grpc import PicoscopeServiceServicer
from h2pcontrol.sdk.server import Server
from pypicosdk import CHANNEL, COUPLING, RANGE, TIME_UNIT, ps5000a
from pypicosdk.constants import resolution_literal, trigger_dir_l

logger = logging.getLogger(__name__)


# Maps from protobuf enums to pypicosdk enums
_VOLTAGE_RANGE_MAP: dict[VoltageRange, RANGE] = {
    VOLTAGE_RANGE_10_MV: RANGE.mV10,
    VOLTAGE_RANGE_20_MV: RANGE.mV20,
    VOLTAGE_RANGE_50_MV: RANGE.mV50,
    VOLTAGE_RANGE_100_MV: RANGE.mV100,
    VOLTAGE_RANGE_200_MV: RANGE.mV200,
    VOLTAGE_RANGE_500_MV: RANGE.mV500,
    VOLTAGE_RANGE_1_V: RANGE.V1,
    VOLTAGE_RANGE_2_V: RANGE.V2,
    VOLTAGE_RANGE_5_V: RANGE.V5,
    VOLTAGE_RANGE_10_V: RANGE.V10,
    VOLTAGE_RANGE_20_V: RANGE.V20,
}

_COUPLING_MAP: dict[Coupling, COUPLING] = {
    COUPLING_DC: COUPLING.DC,
    COUPLING_AC: COUPLING.AC,
}

_TRIGGER_DIR_MAP: dict[TriggerDirection, trigger_dir_l] = {
    TRIGGER_DIRECTION_RISING: "rising",
    TRIGGER_DIRECTION_FALLING: "falling",
    TRIGGER_DIRECTION_RISING_OR_FALLING: "rising or falling",
    TRIGGER_DIRECTION_ABOVE: "above",
    TRIGGER_DIRECTION_BELOW: "below",
}

_RESOLUTION_MAP: dict[Resolution, resolution_literal] = {
    RESOLUTION_8_BIT: "8bit",
    RESOLUTION_12_BIT: "12bit",
    RESOLUTION_14_BIT: "14bit",
    RESOLUTION_15_BIT: "15bit",
    RESOLUTION_16_BIT: "16bit",
}


class PicoscopeService(Server, PicoscopeServiceServicer):
    def __init__(self, cfg):
        super().__init__(cfg)
        self._scope = ps5000a()
        self._scope.open_unit()
        logger.info("PicoScope opened: %s", self._scope.get_unit_serial())

        self._timebase_index: int | None = None
        self._pre_trigger_samples: int = 0
        self._post_trigger_samples: int = 0
        self._sample_interval_ns: int = 0

        # The PicoSDK rejects concurrent calls on a single handle
        # (PICO_DRIVER_FUNCTION), so every section that touches the driver has
        # to hold this lock — including readouts that block in a worker thread.
        self._device_lock = asyncio.Lock()

        # Incremented per StreamCaptures call. Only the newest stream may drive
        # the device; older ones (e.g. a previous run whose stream was never
        # closed) observe the change and exit without touching the scope.
        self._stream_generation: int = 0

        self._capture_armed = asyncio.Event()
        self._capture_buffers: dict = {}
        # Number of waveforms armed for the current/last capture. 1 means a
        # normal single block capture; >1 means rapid block mode.
        self._num_captures: int = 1
        # Current device resolution. open_unit() above takes the ps5000a
        # default of 8 bit. Tracked here rather than read back from
        # self._scope.resolution, which stores the mapped int, not the literal.
        self._resolution: resolution_literal = "8bit"

    def _healthy(self) -> bool:
        # Called from the SDK heartbeat loop on every manager ping. ping_unit()
        # is itself a driver call, so issuing it while a capture is in flight
        # trips PICO_DRIVER_FUNCTION and can leave the driver wedged. A held
        # lock means the device is busy, which means it is alive.
        #
        # This is sync, so it cannot be preempted between the check and the
        # call, and every driver call holds the lock — so if the lock is free,
        # no driver call is in flight.
        if self._device_lock.locked():
            return True
        try:
            return self._scope.ping_unit()
        except Exception:
            return False

    # -- Configuration --

    async def ConfigureChannel(
        self, request: ConfigureChannelRequest, context: grpc.aio.ServicerContext
    ) -> ConfigureChannelResponse:
        ch = request.channel

        if not ch.enabled:
            async with self._device_lock:
                self._scope.set_channel(CHANNEL(ch.channel_index), enabled=False)
            logger.info("Channel %d disabled", ch.channel_index)
            return ConfigureChannelResponse()

        coupling = _COUPLING_MAP.get(ch.coupling)
        if coupling is None:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, f"Unsupported coupling: {ch.coupling}"
            )

        voltage_range = _VOLTAGE_RANGE_MAP.get(ch.voltage_range)
        if voltage_range is None:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, f"Unsupported voltage range: {ch.voltage_range}"
            )

        async with self._device_lock:
            self._scope.set_channel(
                channel=CHANNEL(ch.channel_index),
                range=voltage_range,
                enabled=True,
                coupling=coupling,
                offset=ch.analog_offset_volts,
            )
        logger.info(
            "Channel %d configured: range=%s, coupling=%s, offset=%.3g V",
            ch.channel_index,
            voltage_range.name,
            coupling.name,
            ch.analog_offset_volts,
        )
        return ConfigureChannelResponse()

    async def ConfigureTimebase(
        self, request: ConfigureTimebaseRequest, context: grpc.aio.ServicerContext
    ) -> ConfigureTimebaseResponse:
        total_samples = request.num_samples_pre_trigger + request.num_samples_post_trigger
        async with self._device_lock:
            info = self._scope.get_timebase(request.timebase_index, total_samples)

        self._timebase_index = request.timebase_index
        self._pre_trigger_samples = request.num_samples_pre_trigger
        self._post_trigger_samples = request.num_samples_post_trigger
        self._sample_interval_ns = int(info["Interval(ns)"])

        logger.info(
            "Timebase configured: index=%d, interval=%d ns, pre=%d, post=%d samples",
            self._timebase_index,
            self._sample_interval_ns,
            self._pre_trigger_samples,
            self._post_trigger_samples,
        )
        return ConfigureTimebaseResponse(
            timebase_index=request.timebase_index,
            sample_interval_ns=self._sample_interval_ns,
        )

    async def ConfigureResolution(
        self, request: ConfigureResolutionRequest, context: grpc.aio.ServicerContext
    ) -> ConfigureResolutionResponse:
        resolution = _RESOLUTION_MAP.get(request.resolution)
        if resolution is None:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, f"Unsupported resolution: {request.resolution}"
            )

        # Changing resolution requires closing and reopening the unit, which
        # invalidates the device handle and turns every channel off. Skip it
        # when nothing would change, so that re-running an experiment against
        # a long-lived server does not needlessly cycle the device.
        if resolution == self._resolution:
            logger.info("Resolution already %s, leaving device open", resolution)
            return ConfigureResolutionResponse()

        # Stop first: closing a unit mid-capture can fail, and close_unit()
        # discards the driver status, so the failure would pass unnoticed and
        # leave the handle stale.
        async with self._device_lock:
            self._scope.stop()
            self._scope.close_unit()
            self._scope.open_unit(resolution=resolution)
        self._resolution = resolution

        # open_unit() turns all channels off and resets segmented memory, so
        # any buffers and capture config from before the reopen are void.
        # Channels and timebase must be reconfigured after this call.
        self._capture_buffers = {}
        self._num_captures = 1
        self._capture_armed.clear()

        logger.info("Resolution set to %s (device reopened)", resolution)
        return ConfigureResolutionResponse()

    async def ConfigureTrigger(
        self, request: ConfigureTriggerRequest, context: grpc.aio.ServicerContext
    ) -> ConfigureTriggerResponse:
        trig = request.trigger

        if not trig.enabled:
            async with self._device_lock:
                self._scope.set_simple_trigger(CHANNEL(trig.channel_index), enable=False)
            logger.info("Trigger disabled on channel %d", trig.channel_index)
            return ConfigureTriggerResponse()

        direction = _TRIGGER_DIR_MAP.get(trig.direction)
        if direction is None:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"Unsupported trigger direction: {trig.direction}",
            )

        async with self._device_lock:
            self._scope.set_simple_trigger(
                channel=CHANNEL(trig.channel_index),
                threshold=trig.threshold_mv,
                threshold_unit="mv",
                enable=True,
                direction=direction,
                delay=trig.delay_samples,
                auto_trigger=trig.auto_trigger_us,
            )
        logger.info(
            "Trigger configured: ch=%d, dir=%s, threshold=%.1f mV, delay=%d, auto=%d us",
            trig.channel_index,
            direction,
            trig.threshold_mv,
            trig.delay_samples,
            trig.auto_trigger_us,
        )
        return ConfigureTriggerResponse()

    # -- Capture --

    async def ArmCapture(
        self, request: ArmCaptureRequest, context: grpc.aio.ServicerContext
    ) -> ArmCaptureResponse:
        if self._timebase_index is None:
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Timebase not configured")

        total_samples = self._pre_trigger_samples + self._post_trigger_samples
        pre_trig_pct = self._pre_trigger_samples / total_samples * 100 if total_samples > 0 else 0

        # unset/0/1 all mean a normal, single-segment block capture.
        num_captures = request.num_captures if request.num_captures > 1 else 1

        async with self._device_lock:
            if num_captures > 1:
                max_samples_per_segment = self._scope.memory_segments(num_captures)
                if total_samples > max_samples_per_segment:
                    await context.abort(
                        grpc.StatusCode.INVALID_ARGUMENT,
                        f"{total_samples} samples per capture exceeds the "
                        f"{max_samples_per_segment} samples available per segment "
                        f"when using {num_captures} rapid block captures",
                    )
                self._scope.set_no_of_captures(num_captures)
                self._capture_buffers = self._scope.set_data_buffer_for_enabled_channels(
                    total_samples, captures=num_captures
                )
            else:
                # Segment count persists on the device, so a normal capture armed
                # after a rapid block run would otherwise only see 1/n of the
                # memory in segment 0 and silently truncate.
                self._scope.memory_segments(1)
                self._scope.set_no_of_captures(1)
                self._capture_buffers = self._scope.set_data_buffer_for_enabled_channels(
                    total_samples
                )

            self._num_captures = num_captures
            self._scope.run_block_capture(self._timebase_index, total_samples, pre_trig_pct)

        self._capture_armed.set()
        logger.info(
            "Capture armed: timebase=%d, samples=%d (pre=%d, post=%d), captures=%d",
            self._timebase_index,
            total_samples,
            self._pre_trigger_samples,
            self._post_trigger_samples,
            num_captures,
        )
        return ArmCaptureResponse()

    async def StreamCaptures(  # type: ignore[override]
        self, request: StreamCapturesRequest, context: grpc.aio.ServicerContext
    ) -> AsyncIterator[StreamCapturesResponse]:
        total_samples = self._pre_trigger_samples + self._post_trigger_samples
        times = [
            (i - self._pre_trigger_samples) * self._sample_interval_ns * 1e-9
            for i in range(total_samples)
        ]

        # Claim the device. Any stream opened earlier is now stale: a client
        # that dies or is cancelled does not reliably close its stream, so
        # without this the previous run's handler keeps looping and both drive
        # the scope concurrently.
        self._stream_generation += 1
        my_generation = self._stream_generation
        logger.info("StreamCaptures opened (generation %d)", my_generation)

        try:
            while True:
                await self._capture_armed.wait()

                # Checked before clearing, so that when a newer stream displaces
                # this one both wake, this one exits, and the newer one still
                # sees the armed flag.
                if my_generation != self._stream_generation:
                    logger.info(
                        "StreamCaptures generation %d displaced by %d, exiting",
                        my_generation,
                        self._stream_generation,
                    )
                    return

                self._capture_armed.clear()

                # Read out under the lock, but yield outside it: holding the
                # device while the client consumes would stall every other RPC
                # for as long as the client is slow.
                async with self._device_lock:
                    if my_generation != self._stream_generation:
                        return
                    responses = await self._read_captures(total_samples, times)

                for response in responses:
                    yield response
        finally:
            # Only the current owner may stop the device — a displaced stream
            # doing so would kill the capture the new stream just armed.
            if my_generation == self._stream_generation:
                async with self._device_lock:
                    try:
                        self._scope.stop()
                    except Exception:
                        logger.exception("stop() failed while closing StreamCaptures")
            logger.info("StreamCaptures closed (generation %d)", my_generation)

    async def _read_captures(
        self, total_samples: int, times: list[float]
    ) -> list[StreamCapturesResponse]:
        """Read the armed capture(s) off the device. Caller must hold the lock."""
        num_captures = self._num_captures

        if num_captures > 1:
            # Rapid block mode: one hardware readout for the whole batch, then
            # one response per captured segment.
            _actual_samples, overflow_lists = await asyncio.to_thread(
                self._scope.get_values_bulk, total_samples, 0, num_captures - 1
            )
            volts = self._scope.adc_to_volts(self._capture_buffers)
            offsets_ns = await asyncio.to_thread(
                lambda: [
                    self._scope.get_trigger_time_offset(TIME_UNIT.NS, segment_index=i)
                    for i in range(num_captures)
                ]
            )

            ts = Timestamp()
            ts.GetCurrentTime()

            return [
                StreamCapturesResponse(
                    traces=[
                        ChannelTrace(
                            channel_index=ch_name.value,
                            samples=samples[capture_index].tolist(),
                            times_seconds=times,
                            overflow=ch_name in overflow_lists[capture_index],
                        )
                        for ch_name, samples in volts.items()
                    ],
                    trigger_timestamp=ts,
                    capture_index=capture_index,
                    num_captures=num_captures,
                    trigger_time_offset_ns=offsets_ns[capture_index],
                )
                for capture_index in range(num_captures)
            ]

        await asyncio.to_thread(self._scope.get_values, total_samples)

        volts = self._scope.adc_to_volts(self._capture_buffers)
        overflowed = self._scope.is_over_range()

        traces = [
            ChannelTrace(
                channel_index=ch_name.value,
                samples=samples.tolist(),
                times_seconds=times,
                overflow=ch_name in overflowed,
            )
            for ch_name, samples in volts.items()
        ]

        ts = Timestamp()
        ts.GetCurrentTime()

        return [
            StreamCapturesResponse(
                traces=traces,
                trigger_timestamp=ts,
                capture_index=0,
                num_captures=1,
            )
        ]

    # -- Query --

    async def GetTimebases(
        self, request: GetTimebasesRequest, context: grpc.aio.ServicerContext
    ) -> GetTimebasesResponse:
        sample_count = max(self._pre_trigger_samples + self._post_trigger_samples, 1000)
        timebases = []
        async with self._device_lock:
            for i in range(1000):
                try:
                    info = self._scope.get_timebase(i, sample_count)
                    timebases.append(
                        TimebaseInfo(
                            timebase_index=i,
                            sample_interval_ns=int(info["Interval(ns)"]),
                        )
                    )
                except Exception:
                    continue
        return GetTimebasesResponse(timebases=timebases)
