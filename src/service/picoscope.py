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
    GetTimebasesRequest,
    GetTimebasesResponse,
    GetTraceRequest,
    GetTraceResponse,
    StreamTracesRequest,
    StreamTracesResponse,
)
from h2pcontrol.picoscope.v1.picoscope_pb2_grpc import PicoscopeServiceServicer
from h2pcontrol.sdk.server import Server

logger = logging.getLogger(__name__)


class PicoscopeService(Server, PicoscopeServiceServicer):
    def _healthy(self) -> bool:
        return True

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

    async def GetTrace(self, request: GetTraceRequest, context) -> GetTraceResponse:
        raise NotImplementedError

    async def StreamTraces(  # type: ignore[override]
        self, request: StreamTracesRequest, context
    ) -> AsyncIterator[StreamTracesResponse]:
        raise NotImplementedError

    async def GetTimebases(self, request: GetTimebasesRequest, context) -> GetTimebasesResponse:
        raise NotImplementedError
