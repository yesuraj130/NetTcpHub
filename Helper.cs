using System;
using System.ServiceModel;
using System.ServiceModel.Channels;
using System.ServiceModel.Description;
using System.ServiceModel.Dispatcher;

namespace NetTcpHub
{
    internal static class NetTcpServerHelper
    {
        public static ConnectionStatus ToConnectionStatus(this CommunicationState state)
        {
            switch (state)
            {
                case CommunicationState.Created: return ConnectionStatus.NotConnected;
                case CommunicationState.Opening: return ConnectionStatus.Connecting;
                case CommunicationState.Opened: return ConnectionStatus.Connected;
                case CommunicationState.Closing: return ConnectionStatus.Disconnecting;
                case CommunicationState.Closed: return ConnectionStatus.Disconnected;
                case CommunicationState.Faulted: return ConnectionStatus.Faulted;
                default: return ConnectionStatus.NotConnected;
            }
        }

        public static bool ValidateMessage(this HubMessage message, out string error)
        {
            if (message is null) { error = $"{nameof(message)} cannot be null"; return false; }
            if (string.IsNullOrWhiteSpace(message.Data)) { error = $"{nameof(message.Data)} cannot be empty"; return false; }
            if (string.IsNullOrWhiteSpace(message.DataType)) { error = $"{nameof(message.DataType)} cannot be empty"; return false; }
            if (message.MessageId == Guid.Empty) { error = $"{nameof(message.MessageId)} cannot be null"; return false; }
            if (message.CreatedTime == default) { error = $"{nameof(message.CreatedTime)} cannot be empty"; return false; }
            
            error = null; return true;
        }
    }

    public class ClientInputSessionShutdownBehavior : IEndpointBehavior
    {
        private readonly IInputSessionShutdown sessionShutdown;
        public ClientInputSessionShutdownBehavior(IInputSessionShutdown shutdownHandler) => sessionShutdown = shutdownHandler;

        public void ApplyClientBehavior(ServiceEndpoint endpoint, ClientRuntime clientRuntime) => clientRuntime.CallbackDispatchRuntime.InputSessionShutdownHandlers.Add(sessionShutdown);
        public void AddBindingParameters(ServiceEndpoint endpoint, BindingParameterCollection bindingParameters) { }
        public void ApplyDispatchBehavior(ServiceEndpoint endpoint, EndpointDispatcher endpointDispatcher) { }
        public void Validate(ServiceEndpoint endpoint) { }
    }
}

namespace System.Runtime.CompilerServices
{
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    internal static class IsExternalInit { }
}
