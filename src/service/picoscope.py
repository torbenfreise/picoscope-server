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
from pypicosdk import CHANNEL, COUPLING, RANGE, ps5000a
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

        self._capture_armed = asyncio.Event()
        self._capture_buffers: dict = {}

    def _healthy(self) -> bool:
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

        self._scope.close_unit()
        self._scope.open_unit(resolution=resolution)
        logger.info("Resolution set to %s (device reopened)", resolution)
        return ConfigureResolutionResponse()

    async def ConfigureTrigger(
        self, request: ConfigureTriggerRequest, context: grpc.aio.ServicerContext
    ) -> ConfigureTriggerResponse:
        trig = request.trigger

        if not trig.enabled:
            self._scope.set_simple_trigger(CHANNEL(trig.channel_index), enable=False)
            logger.info("Trigger disabled on channel %d", trig.channel_index)
            return ConfigureTriggerResponse()

        direction = _TRIGGER_DIR_MAP.get(trig.direction)
        if direction is None:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"Unsupported trigger direction: {trig.direction}",
            )

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
        self._capture_buffers = self._scope.set_data_buffer_for_enabled_channels(total_samples)

        pre_trig_pct = self._pre_trigger_samples / total_samples * 100 if total_samples > 0 else 0
        self._scope.run_block_capture(self._timebase_index, total_samples, pre_trig_pct)

        self._capture_armed.set()
        logger.info(
            "Capture armed: timebase=%d, samples=%d (pre=%d, post=%d)",
            self._timebase_index,
            total_samples,
            self._pre_trigger_samples,
            self._post_trigger_samples,
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

        try:
            while True:
                await self._capture_armed.wait()
                self._capture_armed.clear()

                logger.info(
                    "get_values: total_samples=%d, handle=%s, buffers=%s, channel_db=%s",
                    total_samples,
                    self._scope.handle,
                    {k: (type(v).__name__, len(v) if hasattr(v, '__len__') else v) for k, v in self._capture_buffers.items()},
                    list(self._scope.channel_db),
                )
                await asyncio.to_thread(self._scope.is_ready)
                logger.info("is_ready completed, calling get_values")
                await asyncio.to_thread(self._scope.get_values, total_samples, 0, 0, 1)

                volts = self._scope.adc_to_volts(self._capture_buffers)
                overflowed = self._scope.is_over_range()

                traces = []
                for ch_name, samples in volts.items():
                    traces.append(
                        ChannelTrace(
                            channel_index=ch_name.value,
                            samples=samples.tolist(),
                            times_seconds=times,
                            overflow=ch_name in overflowed,
                        )
                    )

                ts = Timestamp()
                ts.GetCurrentTime()

                yield StreamCapturesResponse(traces=traces, trigger_timestamp=ts)
        finally:
            try:
                self._scope.stop()
            except Exception:
                pass

    # -- Query --

    async def GetTimebases(
        self, request: GetTimebasesRequest, context: grpc.aio.ServicerContext
    ) -> GetTimebasesResponse:
        sample_count = max(self._pre_trigger_samples + self._post_trigger_samples, 1000)
        timebases = []
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
