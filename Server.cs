using System;
using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using System.IO;
using System.Net;
using System.Net.Security;
using System.ServiceModel;
using System.ServiceModel.Channels;
using System.ServiceModel.Description;
using System.ServiceModel.Dispatcher;
using System.Threading.Tasks;
using NetTcpHub.Client;
using NetTcpHub.Contracts;

namespace NetTcpHub.Server
{
    public class TcpHubServerOptions
    {
        public int Port { get; set; } = 55555;
        public string ServicePath { get; set; } = "NetTcpHub";
        public long MaxReceivedMessageSize { get; set; } = 1024 * 1024;
        public int MaxConnections { get; set; } = 100;
        public bool AllowNtlm { get; set; } = true;
        public bool EnableEncryption { get; set; } = true;
        public bool AutoDeleteTempFile { get; set; } = true;
    }

    public sealed class ConnectionInfo
    {
        internal IContextChannel Channel { get; }
        internal IClientMethods Callback { get; }

        public string SessionId { get; }
        public string IdentityName { get; }
        public string IpAddress { get; }
        public int? Port { get; }
        public DateTime ConnectedAt { get; }

        public object SendLock { get; } = new object();

        private string machineName;
        public string MachineName
        {
            get => machineName ?? IpAddress;
            private set => machineName = value;
        }

        public ConnectionInfo(string sessionId, IContextChannel channel, IClientMethods callback, string identityName, string address, int? port)
        {
            SessionId = sessionId;
            Channel = channel;
            Callback = callback;
            IdentityName = identityName;
            IpAddress = address;
            Port = port;
            ConnectedAt = DateTime.UtcNow;

            // Resolve machine name asynchronously in background to avoid blocking the handshake
            if (!string.IsNullOrWhiteSpace(address))
            {
                Task.Run(() => ResolveMachineNameAsync(address));
            }
        }

        private async Task ResolveMachineNameAsync(string ipAddress)
        {
            try
            {
                var hostEntry = await Dns.GetHostEntryAsync(ipAddress);
                if (!string.IsNullOrWhiteSpace(hostEntry?.HostName))
                {
                    MachineName = hostEntry.HostName;
                }
            }
            catch
            {
                // Fallback to IP address if PTR resolution fails
                MachineName = ipAddress;
            }
        }
    }

    public class TcpHubServer : IDisposable
    {
        [ServiceBehavior(InstanceContextMode = InstanceContextMode.PerSession, ConcurrencyMode = ConcurrencyMode.Multiple)]
        internal class TcpHubServerMethods : IServerMethods
        {
            private readonly TcpHubServer server;
            private readonly ConnectionInfo connectionInfo;

            public TcpHubServerMethods(TcpHubServer server, InstanceContext instanceContext)
            {
                this.server = server;

                var context = OperationContext.Current;
                if (context != null)
                {
                    var endpoint = (RemoteEndpointMessageProperty)context.IncomingMessageProperties[RemoteEndpointMessageProperty.Name];
                    this.connectionInfo = new ConnectionInfo(
                        context.SessionId,
                        context.Channel,
                        context.GetCallbackChannel<IClientMethods>(),
                        context.ServiceSecurityContext?.PrimaryIdentity?.Name,
                        endpoint?.Address,
                        endpoint?.Port);
                    SubscribeEvents();
                }

                if (connectionInfo != null)
                {
                    server.OnClientConnectionChanged(connectionInfo.SessionId, ConnectionStatus.Connected);
                }
            }

            public void Send(HubMessage message) =>
                server.ReceivedMessage(OperationContext.Current?.SessionId, message);

            public void Upload(Stream stream) =>
                server.ReceivedStream(OperationContext.Current?.SessionId, stream);

            private void Channel_Opening(object sender, EventArgs e) =>
                server.OnClientConnectionChanged(connectionInfo?.SessionId, ConnectionStatus.Connecting);

            private void Channel_Opened(object sender, EventArgs e) =>
                server.OnClientConnectionChanged(connectionInfo?.SessionId, ConnectionStatus.Connected);

            private void Channel_Faulted(object sender, EventArgs e)
            {
                UnsubscribeEvents();
                server.OnClientConnectionChanged(connectionInfo?.SessionId, ConnectionStatus.Faulted);
            }

            private void Channel_Closing(object sender, EventArgs e) =>
                server.OnClientConnectionChanged(connectionInfo?.SessionId, ConnectionStatus.Disconnecting);

            private void Channel_Closed(object sender, EventArgs e)
            {
                UnsubscribeEvents();
                server.OnClientConnectionChanged(connectionInfo?.SessionId, ConnectionStatus.Disconnected);
            }

            private void SubscribeEvents()
            {
                if (connectionInfo?.Channel != null)
                {
                    connectionInfo.Channel.Opening += Channel_Opening;
                    connectionInfo.Channel.Opened += Channel_Opened;
                    connectionInfo.Channel.Faulted += Channel_Faulted;
                    connectionInfo.Channel.Closing += Channel_Closing;
                    connectionInfo.Channel.Closed += Channel_Closed;
                    server.RegisterClient(connectionInfo.SessionId, connectionInfo);
                }
            }

            private void UnsubscribeEvents()
            {
                if (connectionInfo?.Channel != null)
                {
                    connectionInfo.Channel.Opening -= Channel_Opening;
                    connectionInfo.Channel.Opened -= Channel_Opened;
                    connectionInfo.Channel.Faulted -= Channel_Faulted;
                    connectionInfo.Channel.Closing -= Channel_Closing;
                    connectionInfo.Channel.Closed -= Channel_Closed;
                    server.UnregisterClient(connectionInfo.SessionId);
                }
            }
        }

        private readonly ConcurrentDictionary<string, ConnectionInfo> connections =
            new ConcurrentDictionary<string, ConnectionInfo>();

        private readonly CustomBinding customBinding;
        private readonly Uri address;
        private readonly string servicePath;
        private readonly bool autoDeleteTempFile;
        private ServiceHost host;
        private bool disposed;

        public bool IsRunning => host != null && host.State == CommunicationState.Opened;

        public TcpHubServer(TcpHubServerOptions options)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));
            if (string.IsNullOrWhiteSpace(options.ServicePath)) throw new ArgumentException("ServicePath is required.");

            autoDeleteTempFile = options.AutoDeleteTempFile;

            var binding = new NetTcpBinding(SecurityMode.Transport);
            binding.Security.Transport.ClientCredentialType = TcpClientCredentialType.Windows;
            binding.Security.Transport.ProtectionLevel = options.EnableEncryption
                ? ProtectionLevel.EncryptAndSign
                : ProtectionLevel.Sign;
            binding.MaxReceivedMessageSize = options.MaxReceivedMessageSize;
            binding.MaxConnections = options.MaxConnections;

            customBinding = new CustomBinding(binding);
            var interceptElement = new ConnectionInterceptBindingElement();
            interceptElement.ClientConnected += InterceptElement_ClientConnected;
            customBinding.Elements.Insert(customBinding.Elements.Count - 1, interceptElement);

            address = new Uri($"net.tcp://0.0.0.0:{options.Port}/");
            servicePath = options.ServicePath;
        }

        private void InterceptElement_ClientConnected(IChannel channel)
        {
            if (channel is IDuplexSessionChannel sessionChannel)
            {
                OnClientConnectionChanged(sessionChannel.Session?.Id, ConnectionStatus.Connecting);
            }
        }

        public void Start()
        {
            if (IsRunning) return;

            host = new ServiceHost(typeof(TcpHubServerMethods), address);
            host.Description.Behaviors.Add(new TcpHubServiceBehavior(this));
            host.AddServiceEndpoint(typeof(IServerMethods), customBinding, servicePath);

            host.Opening += Host_Opening;
            host.Opened += Host_Opened;
            host.Faulted += Host_Faulted;
            host.Closing += Host_Closing;
            host.Closed += Host_Closed;

            host.Open();
        }

        private void Host_Opening(object sender, EventArgs e) => OnConnectionChanged(ConnectionStatus.Connecting);
        private void Host_Opened(object sender, EventArgs e) => OnConnectionChanged(ConnectionStatus.Connected);
        private void Host_Faulted(object sender, EventArgs e) => OnConnectionChanged(ConnectionStatus.Faulted);
        private void Host_Closing(object sender, EventArgs e) => OnConnectionChanged(ConnectionStatus.Disconnecting);
        private void Host_Closed(object sender, EventArgs e) => OnConnectionChanged(ConnectionStatus.Disconnected);

        public event EventHandlerExt<TcpHubServer, MessageReceivedEventArgs> MessageReceived;
        public event EventHandlerExt<TcpHubServer, FileReceivedEventArgs> FileReceived;
        public event EventHandlerExt<TcpHubServer, ConnectionChangedEventArgs> ConnectionChanged;
        public event EventHandlerExt<TcpHubServer, ConnectionChangedEventArgs> ClientConnectionChanged;

        public void SendMessage(string sessionId, HubMessage message)
        {
            if (string.IsNullOrWhiteSpace(sessionId)) return;
            ValidateMessage(message);

            if (connections.TryGetValue(sessionId, out var connectionInfo))
            {
                lock (connectionInfo.SendLock)
                {
                    connectionInfo.Callback.Send(message);
                }
            }
        }

        public void SendFile(string sessionId, string filePath)
        {
            if (string.IsNullOrWhiteSpace(sessionId)) return;
            if (string.IsNullOrWhiteSpace(filePath)) throw new ArgumentNullException(nameof(filePath));
            if (!File.Exists(filePath)) throw new FileNotFoundException("File not found.", filePath);

            if (connections.TryGetValue(sessionId, out var connectionInfo))
            {
                lock (connectionInfo.SendLock)
                {
                    using (FileStream fs = File.OpenRead(filePath))
                    {
                        connectionInfo.Callback.Upload(fs);
                    }
                }
            }
        }

        public void Broadcast(HubMessage message)
        {
            ValidateMessage(message);

            foreach (var kvp in connections)
            {
                try
                {
                    lock (kvp.Value.SendLock)
                    {
                        kvp.Value.Callback.Send(message);
                    }
                }
                catch
                {
                    UnregisterClient(kvp.Key);
                }
            }
        }

        private void ReceivedMessage(string sessionId, HubMessage message)
        {
            try
            {
                ValidateMessage(message);
            }
            catch
            {
                return;
            }

            if (sessionId != null && connections.TryGetValue(sessionId, out var connectionInfo))
            {
                OnMessageReceived(sessionId, connectionInfo.IdentityName, message);
            }
            else
            {
                OnMessageReceived(sessionId, null, message);
            }
        }

        private void ReceivedStream(string sessionId, Stream stream)
        {
            string tempFile = Path.GetTempFileName();
            try
            {
                using (var fileStream = new FileStream(tempFile, FileMode.Open, FileAccess.ReadWrite, FileShare.Read))
                {
                    stream.CopyTo(fileStream);
                }

                OnFileReceived(sessionId, tempFile);
            }
            finally
            {
                if (autoDeleteTempFile && File.Exists(tempFile))
                {
                    try { File.Delete(tempFile); } catch { }
                }
            }
        }

        internal void RegisterClient(string sessionId, ConnectionInfo callback)
        {
            if (!string.IsNullOrWhiteSpace(sessionId))
            {
                connections.TryAdd(sessionId, callback);
            }
        }

        internal void UnregisterClient(string sessionId)
        {
            if (!string.IsNullOrWhiteSpace(sessionId))
            {
                connections.TryRemove(sessionId, out _);
            }
        }

        private static void ValidateMessage(HubMessage message)
        {
            if (message == null) throw new ArgumentNullException(nameof(message), "Message cannot be null.");
            if (string.IsNullOrWhiteSpace(message.Data)) throw new ArgumentException("Data cannot be empty.");
            if (message.Data.Length > 100000) throw new ArgumentException("Data exceeds maximum length.");
            if (string.IsNullOrWhiteSpace(message.DataType)) throw new ArgumentException("DataType required.");
            if (message.MessageId == Guid.Empty) message.MessageId = Guid.NewGuid();
            if (message.CreatedUtc == default) message.CreatedUtc = DateTime.UtcNow;
        }

        private void OnMessageReceived(string sessionId, string userName, HubMessage message) =>
            MessageReceived?.Invoke(this, new MessageReceivedEventArgs { SessionId = sessionId, UserName = userName, Message = message });

        private void OnConnectionChanged(ConnectionStatus connectionStatus) =>
            ConnectionChanged?.Invoke(this, new ConnectionChangedEventArgs(connectionStatus));

        private void OnClientConnectionChanged(string sessionId, ConnectionStatus connectionStatus) =>
            ClientConnectionChanged?.Invoke(this, new ConnectionChangedEventArgs(connectionStatus) { SessionId = sessionId });

        private void OnFileReceived(string sessionId, string filePath) =>
            FileReceived?.Invoke(this, new FileReceivedEventArgs { SessionId = sessionId, FilePath = filePath });

        public void Stop()
        {
            if (host == null) return;
            try
            {
                if (host.State == CommunicationState.Faulted) host.Abort();
                else host.Close();
            }
            catch { host.Abort(); }
            finally { host = null; }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposed) return;
            if (disposing) Stop();
            disposed = true;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }

    internal class TcpHubInstanceProvider : IInstanceProvider
    {
        private readonly TcpHubServer server;
        public TcpHubInstanceProvider(TcpHubServer server) => this.server = server;

        public object GetInstance(InstanceContext instanceContext)
        {
            return new TcpHubServer.TcpHubServerMethods(server, instanceContext);
        }

        public object GetInstance(InstanceContext instanceContext, Message message) => GetInstance(instanceContext);
        public void ReleaseInstance(InstanceContext instanceContext, object instance)
        {
            if (instance is IDisposable disposable) disposable.Dispose();
        }
    }

    internal sealed class TcpHubServiceBehavior : IServiceBehavior
    {
        private readonly TcpHubServer server;
        public TcpHubServiceBehavior(TcpHubServer server) => this.server = server;

        public void ApplyDispatchBehavior(ServiceDescription serviceDescription, ServiceHostBase serviceHostBase)
        {
            var provider = new TcpHubInstanceProvider(server);

            foreach (ChannelDispatcher dispatcher in serviceHostBase.ChannelDispatchers)
            {
                foreach (EndpointDispatcher endpoint in dispatcher.Endpoints)
                {
                    endpoint.DispatchRuntime.InstanceProvider = provider;
                }
            }
        }

        public void AddBindingParameters(ServiceDescription serviceDescription, ServiceHostBase serviceHostBase, Collection<ServiceEndpoint> endpoints, BindingParameterCollection bindingParameters) { }
        public void Validate(ServiceDescription serviceDescription, ServiceHostBase serviceHostBase) { }
    }

    internal sealed class ConnectionInterceptBindingElement : BindingElement
    {
        public event Action<IChannel> ClientConnected;
        public event Action<IChannel> ClientDisconnected;

        public override IChannelListener<TChannel> BuildChannelListener<TChannel>(BindingContext context) =>
            new InterceptingChannelListener<TChannel>(context.BuildInnerChannelListener<TChannel>(), ClientConnected, ClientDisconnected);

        public override bool CanBuildChannelListener<TChannel>(BindingContext context) =>
            context.CanBuildInnerChannelListener<TChannel>();

        public override BindingElement Clone() =>
            new ConnectionInterceptBindingElement { ClientConnected = this.ClientConnected, ClientDisconnected = this.ClientDisconnected };

        public override T GetProperty<T>(BindingContext context) =>
            context.GetInnerProperty<T>();
    }

    internal sealed class InterceptingChannelListener<TChannel> : ChannelListenerBase<TChannel> where TChannel : class, IChannel
    {
        private readonly IChannelListener<TChannel> innerListener;
        private readonly Action<IChannel> onClientAccepted;
        private readonly Action<IChannel> onClientClosed;

        public InterceptingChannelListener(IChannelListener<TChannel> innerListener, Action<IChannel> onClientAccepted, Action<IChannel> onClientClosed)
        {
            this.innerListener = innerListener;
            this.onClientAccepted = onClientAccepted;
            this.onClientClosed = onClientClosed;
        }

        public override Uri Uri => innerListener.Uri;

        private TChannel WrapAcceptedChannel(TChannel channel)
        {
            if (channel != null)
            {
                onClientAccepted?.Invoke(channel);

                channel.Closed += Channel_Closed;
                channel.Faulted += Channel_Faulted;
            }
            return channel;

            void Channel_Faulted(object sender, EventArgs e)
            {
                channel.Closed -= Channel_Closed;
                channel.Faulted -= Channel_Faulted;
                onClientClosed?.Invoke(channel);
            }

            void Channel_Closed(object sender, EventArgs e)
            {
                channel.Closed -= Channel_Closed;
                channel.Faulted -= Channel_Faulted;
                onClientClosed?.Invoke(channel);
            }
        }

        protected override TChannel OnAcceptChannel(TimeSpan timeout) => WrapAcceptedChannel(innerListener.AcceptChannel(timeout));
        protected override IAsyncResult OnBeginAcceptChannel(TimeSpan timeout, AsyncCallback callback, object state) => innerListener.BeginAcceptChannel(timeout, callback, state);
        protected override TChannel OnEndAcceptChannel(IAsyncResult result) => WrapAcceptedChannel(innerListener.EndAcceptChannel(result));
        protected override bool OnWaitForChannel(TimeSpan timeout) => innerListener.WaitForChannel(timeout);
        protected override IAsyncResult OnBeginWaitForChannel(TimeSpan timeout, AsyncCallback callback, object state) => innerListener.BeginWaitForChannel(timeout, callback, state);
        protected override bool OnEndWaitForChannel(IAsyncResult result) => innerListener.EndWaitForChannel(result);
        protected override void OnOpen(TimeSpan timeout) => innerListener.Open(timeout);
        protected override IAsyncResult OnBeginOpen(TimeSpan timeout, AsyncCallback callback, object state) => innerListener.BeginOpen(timeout, callback, state);
        protected override void OnEndOpen(IAsyncResult result) => innerListener.EndOpen(result);
        protected override void OnClose(TimeSpan timeout) => innerListener.Close(timeout);
        protected override IAsyncResult OnBeginClose(TimeSpan timeout, AsyncCallback callback, object state) => innerListener.BeginClose(timeout, callback, state);
        protected override void OnEndClose(IAsyncResult result) => innerListener.EndClose(result);
        protected override void OnAbort() => innerListener.Abort();
        public override T GetProperty<T>() => innerListener.GetProperty<T>();
    }
}
