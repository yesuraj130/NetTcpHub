using System;
using System.IO;
using System.Runtime.Serialization;
using System.ServiceModel;

namespace NetTcpHub
{
    [ServiceContract(CallbackContract = typeof(IClientMethods), SessionMode = SessionMode.Required)]
    internal interface IServerMethods
    {
        [OperationContract(IsOneWay = true)] void Send(HubMessage message);
        [OperationContract(IsOneWay = true)] void Upload(Stream stream);
    }


    internal interface IClientMethods
    {
        [OperationContract(IsOneWay = true)] void Send(HubMessage message);
        [OperationContract(IsOneWay = true)] void Upload(Stream stream);
    }

    [DataContract]
    public class HubMessage
    {
        [DataMember] public Guid MessageId { get; set; }
        [DataMember] public string DataType { get; set; }
        [DataMember] public string Data { get; set; }
        [DataMember] public DateTime CreatedUtc { get; set; }
    }

    //public enum MessageType
    //{
    //    Text = 0,
    //    Notification = 1,
    //    System = 2
    //}

    internal static class Helper
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

        public static void ValidateMessage(this HubMessage message)
        {
            if (message is null) throw new ArgumentNullException(nameof(message), "Message cannot be null.");
            if (string.IsNullOrWhiteSpace(message.Data)) throw new ArgumentException("Data cannot be empty.");
            if (message.Data.Length > 100000) throw new ArgumentException("Data exceeds maximum length.");
            if (string.IsNullOrWhiteSpace(message.DataType)) throw new ArgumentException("DataType required.");
            if (message.MessageId == Guid.Empty) message.MessageId = Guid.NewGuid();
            if (message.CreatedUtc == default) message.CreatedUtc = DateTime.UtcNow;


            //string authenticatedUser = GetAuthenticatedUser();

            //if (!string.Equals(authenticatedUser, username, StringComparison.OrdinalIgnoreCase)) throw new FaultException("Identity mismatch.");
        }
    }


    ///<summary>Represents the state of a connection.</summary>
    public enum ConnectionStatus
    {
        ///<summary>The connection channel created, but is not yet attempted to connect.</summary>
        NotConnected,

        ///<summary>The connection channel is in the process of connecting.</summary>
        Connecting,

        ///<summary>The connection channel is connected and ready for communication.</summary>
        Connected,

        ///<summary>The connection channel is in the process of disconnecting.</summary>
        Disconnecting,

        ///<summary>The connection channel has been disconnected and can no longer be used.</summary>
        Disconnected,

        ///<summary>The connection channel encountered an unrecoverable error and is no longer usable.</summary>
        Faulted,
    }


}