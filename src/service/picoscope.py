import logging
from collections.abc import AsyncIterator

from h2pcontrol.picoscope.v1.picoscope_pb2 import (
    ConfigureChannelRequest,
    ConfigureChannelResponse,
    ConfigureResolutionRequest,
    ConfigureResolutionResponse,
    ConfigureTimebaseRequest,
    ConfigureTimebaseResponse,
    ConfigureTriggerRequest,
    ConfigureTriggerResponse,
    GetResolutionsRequest,
    GetResolutionsResponse,
    GetTimebasesRequest,
    GetTimebasesResponse,
    GetTraceRequest,
    GetTraceResponse,
    GetVoltageRangesRequest,
    GetVoltageRangesResponse,
    StreamTracesRequest,
    StreamTracesResponse,
)
from h2pcontrol.picoscope.v1.picoscope_pb2_grpc import PicoscopeServiceServicer
from h2pcontrol.sdk.server import Server

logger = logging.getLogger(__name__)


class PicoscopeService(Server, PicoscopeServiceServicer):
    def _healthy(self) -> bool:
        return True

    # Configuration

    async def ConfigureChannel(
        self, request: ConfigureChannelRequest, context
    ) -> ConfigureChannelResponse:
        raise NotImplementedError

    async def ConfigureTrigger(
        self, request: ConfigureTriggerRequest, context
    ) -> ConfigureTriggerResponse:
        raise NotImplementedError

    async def ConfigureTimebase(
        self, request: ConfigureTimebaseRequest, context
    ) -> ConfigureTimebaseResponse:
        raise NotImplementedError

    async def ConfigureResolution(
        self, request: ConfigureResolutionRequest, context
    ) -> ConfigureResolutionResponse:
        raise NotImplementedError

    # Acquisition

    async def GetTrace(self, request: GetTraceRequest, context) -> GetTraceResponse:
        raise NotImplementedError

    async def StreamTraces(  # type: ignore[override]
        self, request: StreamTracesRequest, context
    ) -> AsyncIterator[StreamTracesResponse]:
        raise NotImplementedError

    # Utilities

    async def GetVoltageRanges(
        self, request: GetVoltageRangesRequest, context
    ) -> GetVoltageRangesResponse:
        raise NotImplementedError

    async def GetResolutions(
        self, request: GetResolutionsRequest, context
    ) -> GetResolutionsResponse:
        raise NotImplementedError

    async def GetTimebases(self, request: GetTimebasesRequest, context) -> GetTimebasesResponse:
        raise NotImplementedError
