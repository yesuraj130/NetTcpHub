using System;
using System.IO;
using System.Runtime.Serialization;
using System.ServiceModel;

namespace NetTcpHub.Contracts
{
    [ServiceContract(CallbackContract = typeof(IClientMethods), SessionMode = SessionMode.Required)]
    public interface IServerMethods
    {
        [OperationContract(IsOneWay = true)] void Send(HubMessage message);
        [OperationContract(IsOneWay = true)] void Upload(Stream stream);
    }

    public interface IClientMethods
    {
        [OperationContract(IsOneWay = true)] void Send(HubMessage message);
        [OperationContract(IsOneWay = true)] void Upload(Stream stream);
    }

    [DataContract]
    public class HubMessage
    {
        [DataMember] public Guid MessageId { get; set; } = Guid.NewGuid();
        [DataMember] public string DataType { get; set; }
        [DataMember] public string Data { get; set; }
        [DataMember] public DateTime CreatedUtc { get; set; } = DateTime.UtcNow;
    }

    /// <summary>Represents the state of a connection.</summary>
    public enum ConnectionStatus
    {
        /// <summary>The connection channel is created, but has not yet attempted to connect.</summary>
        NotConnected,

        /// <summary>The connection channel is in the process of connecting.</summary>
        Connecting,

        /// <summary>The connection channel is connected and ready for communication.</summary>
        Connected,

        /// <summary>The connection channel is in the process of disconnecting.</summary>
        Disconnecting,

        /// <summary>The connection channel has been disconnected and can no longer be used.</summary>
        Disconnected,

        /// <summary>The connection channel encountered an unrecoverable error and is no longer usable.</summary>
        Faulted,
    }
}
