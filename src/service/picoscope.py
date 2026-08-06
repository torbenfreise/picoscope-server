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
    Armed,
    CaptureData,
    CaptureRequest,
    CaptureResponse,
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
_VOLTAGE_RANGE_MAP: dict[VoltageRange.ValueType, RANGE] = {
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

_COUPLING_MAP: dict[Coupling.ValueType, COUPLING] = {
    COUPLING_DC: COUPLING.DC,
    COUPLING_AC: COUPLING.AC,
}

_TRIGGER_DIR_MAP: dict[TriggerDirection.ValueType, trigger_dir_l] = {
    TRIGGER_DIRECTION_RISING: "rising",
    TRIGGER_DIRECTION_FALLING: "falling",
    TRIGGER_DIRECTION_RISING_OR_FALLING: "rising or falling",
    TRIGGER_DIRECTION_ABOVE: "above",
    TRIGGER_DIRECTION_BELOW: "below",
}

_RESOLUTION_MAP: dict[Resolution.ValueType, resolution_literal] = {
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

        # True for the duration of a Capture call. The scope belongs to one
        # acquisition at a time, so a second Capture is rejected rather than
        # queued: a queued acquisition would arm at a moment the client cannot
        # predict relative to its own trigger.
        self._acquiring: bool = False

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

        # open_unit() turns all channels off, and the resolution changes which
        # sample intervals the device can reach, so the stored timebase no
        # longer describes the device. Drop it: Capture then fails with
        # FAILED_PRECONDITION until the client reconfigures, rather than
        # acquiring at an interval that differs from the one it was told.
        self._timebase_index = None
        self._sample_interval_ns = 0

        logger.info(
            "Resolution set to %s (device reopened; channels and timebase must be reconfigured)",
            resolution,
        )
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
                threshold=round(trig.threshold_mv),
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

    async def Capture(  # type: ignore[override]
            self, request: CaptureRequest, context: grpc.aio.ServicerContext
    ) -> AsyncIterator[CaptureResponse]:
        if self._timebase_index is None:
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Timebase not configured")

        # Rejected rather than queued: a queued acquisition would arm at a
        # moment the client cannot predict relative to its own trigger.
        if self._acquiring:
            await context.abort(
                grpc.StatusCode.FAILED_PRECONDITION, "An acquisition is already in progress"
            )

        # unset/0/1 all mean a normal, single-segment block capture.
        num_captures = request.num_captures if request.num_captures > 1 else 1
        total_samples = self._pre_trigger_samples + self._post_trigger_samples

        self._acquiring = True
        try:
            async with self._device_lock:
                buffers = await self._arm(num_captures, total_samples, context)

            logger.info(
                "Armed: timebase=%d, samples=%d (pre=%d, post=%d), captures=%d",
                self._timebase_index,
                total_samples,
                self._pre_trigger_samples,
                self._post_trigger_samples,
                num_captures,
            )
            yield CaptureResponse(armed=Armed())

            async with self._device_lock:
                captures = await self._read(buffers, num_captures, total_samples)

            for capture in captures:
                yield CaptureResponse(capture=capture)
        finally:
            self._acquiring = False
            async with self._device_lock:
                try:
                    self._scope.stop()
                except Exception:
                    logger.exception("stop() failed while ending acquisition")

    async def _arm(
            self, num_captures: int, total_samples: int, context: grpc.aio.ServicerContext
    ) -> dict:
        """Configure segments and buffers and start the block capture.

        Caller must hold the device lock. Returns the per-channel buffers.
        """
        assert self._timebase_index is not None
        pre_trig_pct = self._pre_trigger_samples / total_samples * 100 if total_samples > 0 else 0

        # The interval a timebase index resolves to depends on the resolution
        # and on how many channels are enabled, so enabling a channel after
        # ConfigureTimebase can change it. The client builds its time axis from
        # the interval reported at configuration time, so acquiring at a
        # different one would mislabel every sample: refuse instead.
        info = self._scope.get_timebase(self._timebase_index, total_samples)
        interval_ns = int(info["Interval(ns)"])
        if interval_ns != self._sample_interval_ns:
            await context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                f"Timebase {self._timebase_index} now resolves to {interval_ns} ns, "
                f"not the {self._sample_interval_ns} ns reported when it was "
                "configured — reconfigure the timebase after changing channels "
                "or resolution",
            )

        if num_captures > 1:
            max_samples_per_segment = self._scope.memory_segments(num_captures)
            if total_samples > max_samples_per_segment:
                await context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    f"{total_samples} samples per capture exceeds the "
                    f"{max_samples_per_segment} samples available per segment "
                    f"when using {num_captures} rapid block captures",
                )
        else:
            # Segment count persists on the device, so a normal capture taken
            # after a rapid block acquisition would otherwise only see 1/n of
            # the memory in segment 0 and silently truncate.
            self._scope.memory_segments(1)

        self._scope.set_no_of_captures(num_captures)
        buffers = self._scope.set_data_buffer_for_enabled_channels(
            total_samples, captures=num_captures if num_captures > 1 else 0
        )
        self._scope.run_block_capture(self._timebase_index, total_samples, pre_trig_pct)
        return buffers

    async def _read(
            self, buffers: dict, num_captures: int, total_samples: int,
    ) -> list[CaptureData]:
        """Read the armed capture(s) off the device. Caller must hold the lock."""
        if num_captures > 1:
            # Rapid block: one readout for the whole batch, then one message
            # per captured segment.
            actual_samples, overflow_lists = await asyncio.to_thread(
                self._scope.get_values_bulk, total_samples, 0, num_captures - 1
            )
            # The driver may return fewer samples than asked for. The client
            # derives its time axis from the sample count it requested, so a
            # short read would shift every sample label without any visible
            # symptom.
            if actual_samples != total_samples:
                logger.warning(
                    "Short read: driver returned %d of %d samples per capture",
                    actual_samples,
                    total_samples,
                )
            offsets_ns = await asyncio.to_thread(
                lambda: [
                    self._scope.get_trigger_time_offset(TIME_UNIT.NS, segment_index=i)
                    for i in range(num_captures)
                ]
            )
        else:
            await asyncio.to_thread(self._scope.get_values, total_samples)
            overflow_lists = [self._scope.is_over_range()]
            offsets_ns = [0]

        volts = self._scope.adc_to_volts(buffers)
        # adc_to_volts is annotated dict | float | np.ndarray, but a dict buffer
        # in always yields a dict out (keyed by CHANNEL enum members).
        assert isinstance(volts, dict)
        # Host clock at readout, shared by every capture in the batch: an
        # approximate wall-clock label for the acquisition, not the instant a
        # trigger fired. Device-accurate relative timing is in each capture's
        # trigger_time_offset_ns.
        ts = Timestamp()
        ts.GetCurrentTime()

        return [
            CaptureData(
                traces=[
                    ChannelTrace(
                        channel_index=ch_name.value,
                        samples=(samples[i] if num_captures > 1 else samples).tolist(),
                        # is_over_range() reports channel names ("A", "B"),
                        # while volts is keyed by CHANNEL enum members.
                        overflow=ch_name.name in overflow_lists[i],
                    )
                    for ch_name, samples in volts.items()
                ],
                trigger_timestamp=ts,
                capture_index=i,
                trigger_time_offset_ns=offsets_ns[i],
            )
            for i in range(num_captures)
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
