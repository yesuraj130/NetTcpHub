using System;
using System.IO;
using System.Runtime.Serialization;
using System.ServiceModel;

namespace NetTcpHub
{
    #region Options
    public record TcpHubServerOptions
    {
        public int Port { get; init; }
        public string ServicePath { get; init; }

        public bool AllowNtlm { get; init; } = false;
        public bool EnableEncryption { get; init; } = true;

        public long MaxReceivedMessageSize { get; init; } = 1024 * 1024;
        public int MaxConnections { get; init; } = 100;
    }

    public record TcpHubClientOptions
    {
        public string ServerName { get; init; }
        public int Port { get; set; }
        public string ServicePath { get; init; }
        public string UserIdentity { get; init; }

        public bool AllowNtlm { get; init; } = false;
        public bool EnableEncryption { get; init; } = true;
        public bool AutoReconnect { get; init; } = false;

        public long MaxReceivedMessageSize { get; init; } = 1024 * 1024;
        public int OpenTimeoutSeconds { get; init; } = 30;
        public int SendTimeoutSeconds { get; init; } = 60;
        public int ReceiveTimeoutMinutes { get; init; } = 10;
        public int CloseTimeoutSeconds { get; init; } = 30;

    }
    #endregion


    #region EventArgs
    public delegate void EventHandlerExt<TSender, TEventArgs>(TSender sender, TEventArgs e);

    public class MessageReceivedEventArgs : EventArgs
    {
        public MessageReceivedEventArgs(string userName, string sessionId, HubMessage message)
        {
            UserName = userName;
            SessionId = sessionId;
            Message = message;
        }

        public string UserName { get; }
        public string SessionId { get; }
        public HubMessage Message { get; }
    }

    public class FileReceivedEventArgs : EventArgs
    {
        public FileReceivedEventArgs(string sessionId, string filePath, HubMessage message, bool autoDelete)
        {
            SessionId = sessionId;
            FilePath = filePath;
            AutoDelete = autoDelete;
            Message = message;
        }

        public string SessionId { get; }
        public string FilePath { get; }
        public bool AutoDelete { get; set; }
        public HubMessage Message { get; }
    }

    public class ConnectionChangedEventArgs : EventArgs
    {
        public string SessionId { get; }
        public ConnectionStatus Status { get; }

        public ConnectionChangedEventArgs(string sessionId, ConnectionStatus connectionStatus)
        {
            SessionId = sessionId;
            Status = connectionStatus;
        }
    }
    #endregion


    #region DataStructure
    [DataContract]
    public class HubMessage
    {
        public HubMessage(Guid messageId, string dataType, string data)
        {
            MessageId = messageId;
            DataType = dataType;
            Data = data;
            CreatedTime = DateTime.Now;
        }

        [DataMember] public Guid MessageId { get; }
        [DataMember] public string DataType { get; }
        [DataMember] public string Data { get; }
        [DataMember] public DateTime CreatedTime { get; }
    }

    [MessageContract]
    public class FileUploadRequest
    {
        public FileUploadRequest() { }
        public FileUploadRequest(HubMessage hubMessage, string fileName, long fileSize, Stream fileStream)
        {
            HubMessage = hubMessage;
            FileName = fileName;
            FileSize = fileSize;
            FileStream = fileStream;
        }

        [MessageHeader(MustUnderstand = true)] public HubMessage HubMessage { get; private set; }
        [MessageHeader(MustUnderstand = true)] public string FileName { get; private set; }
        [MessageHeader(MustUnderstand = true)] public long FileSize { get; private set; }
        [MessageBodyMember(Order = 1)] public Stream FileStream { get; private set; }
    }
    #endregion


    #region Enums
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
    #endregion
}