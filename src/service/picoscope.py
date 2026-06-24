import logging
from collections.abc import AsyncIterator

from h2pcontrol.picoscope.v1.picoscope_pb2 import (
    ArmCaptureRequest,
    ArmCaptureResponse,
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
    StreamCapturesRequest,
    StreamCapturesResponse,
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

    async def ArmCapture(self, request: ArmCaptureRequest, context) -> ArmCaptureResponse:
        raise NotImplementedError

    async def StreamCaptures(  # type: ignore[override]
        self, request: StreamCapturesRequest, context
    ) -> AsyncIterator[StreamCapturesResponse]:
        raise NotImplementedError

    async def GetTimebases(self, request: GetTimebasesRequest, context) -> GetTimebasesResponse:
        raise NotImplementedError
