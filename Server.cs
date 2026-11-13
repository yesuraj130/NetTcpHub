using System;
using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using System.ServiceModel;
using System.ServiceModel.Channels;
using System.ServiceModel.Description;
using System.ServiceModel.Dispatcher;
using System.IO;
using System.Net;
using System.Reflection;
using System.ServiceModel.Security;
using System.Security.Principal;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace NetTcpHub
{

    #region Internal Types
    [ServiceContract(CallbackContract = typeof(IClientMethods), SessionMode = SessionMode.Required)]
    internal interface IServerMethods
    {
        [OperationContract(IsOneWay = true)] void Send(HubMessage message);
        [OperationContract(IsOneWay = true)] void Upload(FileUploadRequest stream);
    }

    internal sealed class ConnectionInfo
    {
        internal IChannel Channel { get; private set; }
        internal IClientMethods Callback { get; private set; }

        internal ConnectionInfo(string sessionId, IChannel channel, IClientMethods callback, string identityName, string address, int? port)
        {
            SessionId = sessionId;
            Channel = channel;
            Callback = callback;
            IdentityName = identityName;
            IpAddress = address;
            Port = port;
            ConnectedAt = DateTime.UtcNow;

            if (!string.IsNullOrWhiteSpace(address)) System.Threading.Tasks.Task.Run(() => ResolveMachineNameAsync(address));
        }

        public string SessionId { get; }
        public string IdentityName { get; private set; }
        public string IpAddress { get; private set; }
        public int? Port { get; private set; }
        public DateTime ConnectedAt { get; }

        internal void UpdateCallBack(IContextChannel channel, IClientMethods callback)
        {
            Channel = channel;
            Callback = callback;
        }
        internal void UpdateIpAddress(string identityName, string address, int? port)
        {
            var addressChanged = !string.Equals(IpAddress, address);
            IdentityName = identityName;
            IpAddress = address;
            Port = port;

            if (addressChanged && !string.IsNullOrWhiteSpace(address)) System.Threading.Tasks.Task.Run(() => ResolveMachineNameAsync(address));
        }

        private string machineName;
        public string MachineName => machineName ?? IpAddress;
        private async System.Threading.Tasks.Task ResolveMachineNameAsync(string ipAddress)
        {
            try
            {
                System.Net.IPHostEntry hostEntry = await System.Net.Dns.GetHostEntryAsync(ipAddress);//Resolves IP to FQDN
                if (!string.IsNullOrWhiteSpace(hostEntry?.HostName)) machineName = hostEntry.HostName;
            }
            catch { }
        }
        internal object SendLock { get; } = new object(); // Protects concurrent writes to this specific client's callback channel
    }
    #endregion


    public class TcpHubServer : IDisposable
    {
        #region Private Classes
        [ServiceBehavior(InstanceContextMode = InstanceContextMode.PerSession, ConcurrencyMode = ConcurrencyMode.Multiple)]
        internal sealed class TcpHubServerMethods : IServerMethods, IDisposable
        {
            private readonly TcpHubServer server;
            private readonly ConnectionInfo connectionInfo;
            public TcpHubServerMethods(TcpHubServer server)
            {
                this.server = server;

                var context = OperationContext.Current;
                if (!string.IsNullOrWhiteSpace(context?.SessionId))
                {
                    var callback = context.GetCallbackChannel<IClientMethods>();
                    var endpoint = (RemoteEndpointMessageProperty)context.IncomingMessageProperties[RemoteEndpointMessageProperty.Name];
                    var identity = context.ServiceSecurityContext?.PrimaryIdentity;

                    this.connectionInfo = server.GetConnectionInfo(context.SessionId);
                    if (connectionInfo is null)
                    {
                        this.connectionInfo = new ConnectionInfo(context.SessionId, context.Channel, callback, identity?.Name, endpoint?.Address, endpoint?.Port);
                        server.RegisterClient(context.SessionId, connectionInfo);
                        SubscribeEvents();
                        server.OnClientConnectionChanged(connectionInfo?.SessionId, ConnectionStatus.Connected);
                    }
                    else
                    {
                        connectionInfo.UpdateCallBack(context.Channel, callback);
                        connectionInfo.UpdateIpAddress(identity?.Name, endpoint?.Address, endpoint?.Port);
                    }
                }
            }

            public void Send(HubMessage message) => server.ReceivedMessage(connectionInfo?.SessionId, message);
            public void Upload(FileUploadRequest stream) => server.ReceivedStream(connectionInfo?.SessionId, stream);

            private void Channel_Opening(object sender, EventArgs e) => server.OnClientConnectionChanged(connectionInfo?.SessionId, ConnectionStatus.Connecting);
            private void Channel_Opened(object sender, EventArgs e) => server.OnClientConnectionChanged(connectionInfo?.SessionId, ConnectionStatus.Connected);
            private void Channel_Faulted(object sender, EventArgs e)
            {
                UnsubscribeEvents();
                server.OnClientConnectionChanged(connectionInfo?.SessionId, ConnectionStatus.Faulted);
            }
            private void Channel_Closing(object sender, EventArgs e) => server.OnClientConnectionChanged(connectionInfo?.SessionId, ConnectionStatus.Disconnecting);
            private void Channel_Closed(object sender, EventArgs e)
            {
                UnsubscribeEvents();
                server.OnClientConnectionChanged(connectionInfo?.SessionId, ConnectionStatus.Disconnected);
            }

            private void SubscribeEvents()
            {
                if (connectionInfo?.Channel is not null)
                {
                    connectionInfo.Channel.Opening += Channel_Opening;
                    connectionInfo.Channel.Opened += Channel_Opened;
                    connectionInfo.Channel.Faulted += Channel_Faulted;
                    connectionInfo.Channel.Closing += Channel_Closing;
                    connectionInfo.Channel.Closed += Channel_Closed;
                }
            }
            private void UnsubscribeEvents()
            {
                if (connectionInfo?.Channel is not null)
                {
                    connectionInfo.Channel.Opening -= Channel_Opening;
                    connectionInfo.Channel.Opened -= Channel_Opened;
                    connectionInfo.Channel.Faulted -= Channel_Faulted;
                    connectionInfo.Channel.Closing -= Channel_Closing;
                    connectionInfo.Channel.Closed -= Channel_Closed;
                    server.UnregisterClient(connectionInfo.SessionId);
                }
                if (connectionInfo is not null) server.UnregisterClient(connectionInfo.SessionId);
            }
            public void Dispose() => UnsubscribeEvents();
        }
        #endregion


        #region Fields
        private readonly ConcurrentDictionary<string, ConnectionInfo> connections = new ConcurrentDictionary<string, ConnectionInfo>();
        private readonly CustomBinding customBinding;
        private readonly InterceptingBindingElement interceptElement;
        private readonly TcpHubServerOptions options;
        private readonly object hostLock = new object();
#pragma warning disable CA2213 // Disposable fields should be disposed
        private ServiceHost host;
#pragma warning restore CA2213 // Disposable fields should be disposed
        #endregion


        #region Constructor

        private TcpHubServer(TcpHubServerOptions serverOptions)
        {
            this.options = serverOptions;

            var binding = new NetTcpBinding(SecurityMode.Transport);
            binding.Security.Transport.ClientCredentialType = TcpClientCredentialType.Windows;
            binding.Security.Transport.ProtectionLevel = serverOptions.EnableEncryption ? System.Net.Security.ProtectionLevel.EncryptAndSign : System.Net.Security.ProtectionLevel.Sign;
            binding.MaxReceivedMessageSize = serverOptions.MaxReceivedMessageSize;
            binding.MaxConnections = serverOptions.MaxConnections;
            binding.OpenTimeout = TimeSpan.FromSeconds(serverOptions.OpenTimeoutSeconds);
            binding.SendTimeout = TimeSpan.FromSeconds(serverOptions.SendTimeoutSeconds);
            binding.ReceiveTimeout = TimeSpan.FromMinutes(serverOptions.ReceiveTimeoutMinutes);
            binding.CloseTimeout = TimeSpan.FromSeconds(serverOptions.CloseTimeoutSeconds);
            //binding.ReliableSession.Enabled = true;
            //binding.ReliableSession.InactivityTimeout = TimeSpan.FromMinutes(5);

            customBinding = new CustomBinding(binding);
            interceptElement = new InterceptingBindingElement();
            interceptElement.ConnectionChanged += InterceptElement_ConnectionChanged;
            var securityIndex = customBinding.Elements.IndexOf(customBinding.Elements.Find<WindowsStreamSecurityBindingElement>());
            if (securityIndex >= 0) customBinding.Elements.Insert(securityIndex, interceptElement);
            else customBinding.Elements.Insert(customBinding.Elements.Count - 1, interceptElement);
        }
        public static TcpHubServer Create(TcpHubServerOptions serverOptions, out string error)
        {
            if (!ValidateServerOptions(serverOptions, out error)) return null;
            return new TcpHubServer(serverOptions);
        }
        private static bool ValidateServerOptions(TcpHubServerOptions serverOptions, out string error)
        {
            if (serverOptions is null) { error = $"{nameof(serverOptions)} cannot be null"; return false; }
            if (serverOptions.Port < 1000 || serverOptions.Port > 65535) { error = $"{nameof(serverOptions.Port)} must be between 1000 and 65535"; return false; }
            if (string.IsNullOrWhiteSpace(serverOptions.ServicePath)) { error = $"{nameof(serverOptions.ServicePath)} cannot be empty"; return false; }
            error = null; return true;
        }
        #endregion


        #region Public
        public bool IsRunning => host is not null && host.State == CommunicationState.Opened;
        public ConnectionStatus Status => host is null ? ConnectionStatus.NotConnected : host.State.ToConnectionStatus();

        public bool Start(out string error)
        {
            lock (hostLock)
            {
                if (disposed) { error = "already disposed"; return false; }
                if (IsRunning) { error = $"already running"; return false; }

                try
                {
                    host = new ServiceHost(typeof(TcpHubServerMethods), new Uri($"net.tcp://0.0.0.0:{options.Port}/"));
                    host.Description.Behaviors.Add(new TcpHubServiceBehavior(this));
                    host.AddServiceEndpoint(typeof(IServerMethods), customBinding, options.ServicePath);

                    host.Opening += Host_Opening;
                    host.Opened += Host_Opened;
                    host.Faulted += Host_Faulted;
                    host.Closing += Host_Closing;
                    host.Closed += Host_Closed;

                    host.Open();
                    error = null; return true;
                }
                catch (Exception ex)
                {
                    CleanUp();
                    error = $"{ex.Message}.\n\n{ex}";
                    return false;
                }
            }
        }
        //private bool isUserDisconnecting;
        public void Stop()
        {
            //isUserDisconnecting = true;
            lock (hostLock) { CleanUp(); }
        }
        #endregion


        #region Events
        private void Host_Opening(object sender, EventArgs e) => OnConnectionChanged(ConnectionStatus.Connecting);
        private void Host_Opened(object sender, EventArgs e) => OnConnectionChanged(ConnectionStatus.Connected);
        private void Host_Faulted(object sender, EventArgs e) => OnConnectionChanged(ConnectionStatus.Faulted);
        private void Host_Closing(object sender, EventArgs e) => OnConnectionChanged(ConnectionStatus.Disconnecting);
        private void Host_Closed(object sender, EventArgs e) => OnConnectionChanged(ConnectionStatus.Disconnected);
        private void InterceptElement_ConnectionChanged(IChannel channel, ConnectionStatus connectionStatus)
        {
            if (channel is IDuplexSessionChannel sessionChannel)
            {
                OnClientConnectionChanged(sessionChannel.Session?.Id, connectionStatus);

                if (connectionStatus is ConnectionStatus.Connected)
                {
                    if (!string.IsNullOrWhiteSpace(sessionChannel.Session?.Id))
                    {
                        var identity = GetClientIdentity(sessionChannel);
                        var endPoint = GetRemoteEndPoint(sessionChannel);
                        var connectionInfo = new ConnectionInfo(sessionChannel.Session.Id, sessionChannel, null, identity?.Name, $"{endPoint?.Address}", endPoint?.Port);
                        RegisterClient(sessionChannel.Session.Id, connectionInfo);
                    }
                }
            }
            else OnClientConnectionChanged(null, ConnectionStatus.Connecting);
        }

#pragma warning disable CA1003 // Use generic event handler instances
        public event EventHandlerExt<TcpHubServer, MessageReceivedEventArgs> MessageReceived;
        public event EventHandlerExt<TcpHubServer, FileReceivedEventArgs> FileReceived;
        public event EventHandlerExt<TcpHubServer, ConnectionChangedEventArgs> ConnectionChanged;
        public event EventHandlerExt<TcpHubServer, ConnectionChangedEventArgs> ClientConnectionChanged;
#pragma warning restore CA1003 // Use generic event handler instances

        private void OnMessageReceived(string sessionId, string userName, HubMessage message) => MessageReceived?.Invoke(this, new MessageReceivedEventArgs(userName, sessionId, message));
        private void OnConnectionChanged(ConnectionStatus connectionStatus) => ConnectionChanged?.Invoke(this, new ConnectionChangedEventArgs(null, connectionStatus));
        private void OnClientConnectionChanged(string sessionId, ConnectionStatus connectionStatus)
        {
            if (connectionStatus is ConnectionStatus.Disconnected or ConnectionStatus.Faulted) UnregisterClient(sessionId);
            ClientConnectionChanged?.Invoke(this, new ConnectionChangedEventArgs(sessionId, connectionStatus));
        }
        private void OnFileReceived(FileReceivedEventArgs eventArgs) => FileReceived?.Invoke(this, eventArgs);
        #endregion


        #region Send and Receive
        private bool ValidateMessage(HubMessage message, out string error)
        {
            if (!message.ValidateMessage(out error)) return false;
            if (Encoding.UTF8.GetByteCount(message.Data) > options.MaxReceivedMessageSize) { error = $"{nameof(message.Data)} cannot be more than {options.MaxReceivedMessageSize}"; return false; }
            return true;
        }
        public bool SendMessage(string sessionId, HubMessage message, out string error)
        {
            if (!IsRunning) { error = "server not running"; return false; }
            if (string.IsNullOrWhiteSpace(sessionId)) { error = $"{nameof(sessionId)} cannot be empty."; return false; }
            if (!ValidateMessage(message, out error)) return false;

            var connectionInfo = GetConnectionInfo(sessionId);
            if (connectionInfo is not null)
            {
                try
                {
                    lock (connectionInfo.SendLock)
                    {
                        var callback = connectionInfo.Callback;
                        if (callback is null) { error = $"callback not initialized for {sessionId}"; return false; }
                        callback.Send(message);
                    }
                    return true;
                }
                catch (Exception ex)
                {
                    error = $"{ex.Message}.\n\n{ex}";
                    return false;
                }
            }
            else { error = $"connection not found for {sessionId}"; return false; }
        }
        public bool Broadcast(HubMessage message, out string error)
        {
            if (!IsRunning) { error = "server not running"; return false; }
            if (!ValidateMessage(message, out error)) return false;

            var errorMessage = new ConcurrentBag<string>();
            Parallel.ForEach(connections.Values, connectionInfo =>
            {
                if (connectionInfo is not null)
                {
                    try
                    {
                        lock (connectionInfo.SendLock)
                        {
                            var callback = connectionInfo.Callback;
                            if (callback is null) { errorMessage.Add($"{connectionInfo.SessionId}: callback not initialized"); return; }
                            callback.Send(message);
                        }
                    }
                    catch (Exception ex)
                    {
                        errorMessage.Add($"{connectionInfo.SessionId}: {ex.Message}");
                    }
                }
            });

            if (errorMessage.Count > 0)
            {
                error = string.Join("\n", errorMessage); return false;
            }
            else
            {
                error = null; return true;
            }
        }
        public bool SendFile(string sessionId, string filePath, HubMessage message, out string error)
        {
            if (!IsRunning) { error = "server not running"; return false; }
            if (string.IsNullOrWhiteSpace(sessionId)) { error = $"{nameof(sessionId)} cannot be empty."; return false; }
            if (!ValidateMessage(message, out error)) return false;
            if (string.IsNullOrWhiteSpace(filePath) || !File.Exists(filePath)) { error = "File not exists"; return false; }

            var connectionInfo = GetConnectionInfo(sessionId);
            if (connectionInfo is not null)
            {
                try
                {
                    using (var fileStream = File.OpenRead(filePath))
                    {
                        if (fileStream.Length + Encoding.UTF8.GetByteCount(message.Data) > options.MaxReceivedMessageSize) { error = $"File and message size cannot be more than {options.MaxReceivedMessageSize}"; return false; }
                        var request = new FileUploadRequest(message, Path.GetFileName(filePath), fileStream.Length, fileStream);
                        lock (connectionInfo.SendLock)
                        {
                            var callback = connectionInfo.Callback;
                            if (callback is null) { error = $"callback not initialized for {sessionId}"; return false; }
                            callback.Upload(request);
                        }
                    }
                    return true;
                }
                catch (Exception ex)
                {
                    error = $"{ex.Message}.\n\n{ex}";
                    return false;
                }
            }
            else { error = $"connection not found for {sessionId}"; return false; }
        }

        private bool ReceivedMessage(string sessionId, HubMessage message)
        {
            if (!message.ValidateMessage(out var error)) { OnMessageReceived(sessionId, "", new HubMessage(message?.MessageId ?? Guid.NewGuid(), "Error", error)); return false; }

            var connectionInfo = GetConnectionInfo(sessionId);
            if (connectionInfo is not null) { OnMessageReceived(sessionId, connectionInfo.IdentityName, message); return true; }
            else { OnMessageReceived(sessionId, "", new HubMessage(message.MessageId, "Error", $"Unknown message received from {sessionId}")); return false; }


        }
        private bool ReceivedStream(string sessionId, FileUploadRequest stream)
        {
            if (stream is null || !stream.HubMessage.ValidateMessage(out var error))
            {
                stream?.FileStream?.Dispose();
                OnFileReceived(new FileReceivedEventArgs(sessionId, null, new HubMessage(stream?.HubMessage?.MessageId ?? Guid.NewGuid(), "Error", stream is null ? $"{nameof(stream)} is null" : error), true));
                return false;
            }
            if (stream.FileStream is null) { OnFileReceived(new FileReceivedEventArgs(sessionId, null, new HubMessage(stream.HubMessage.MessageId, "Error", $"{nameof(stream.FileStream)} is null"), true)); return false; }

            var connectionInfo = GetConnectionInfo(sessionId);
            if (connectionInfo is not null)
            {
                string tempFile = Path.Combine(Path.GetTempPath(), $"NetTcpHub_{Guid.NewGuid():N}.tmp");
                var eventArgs = new FileReceivedEventArgs(sessionId, tempFile, stream.HubMessage, true, Path.GetFileName(stream.FileName));
                try
                {
                    using (stream.FileStream)
                    using (var fileStream = new FileStream(tempFile, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None, 81920, FileOptions.SequentialScan))
                    {
                        stream.FileStream.CopyTo(fileStream);
                    }
                    OnFileReceived(eventArgs); return true;
                }
                catch (Exception ex)
                {
                    OnFileReceived(new FileReceivedEventArgs(sessionId, null, new HubMessage(stream.HubMessage.MessageId, "Error", $"{ex.Message}\n\n{ex}"), true)); return false;
                }
                finally
                {
                    if (eventArgs.AutoDelete && File.Exists(tempFile)) File.Delete(tempFile);
                }
            }
            else
            {
                stream.FileStream.Dispose();
                OnFileReceived(new FileReceivedEventArgs(sessionId, null, new HubMessage(stream.HubMessage.MessageId, "Error", $"Unknown message received from {sessionId}"), true));
                return false;
            }

        }
        #endregion


        #region Client Registry
        private void RegisterClient(string sessionId, ConnectionInfo callback)
        {
            if (!string.IsNullOrWhiteSpace(sessionId)) connections.TryAdd(sessionId, callback);
        }
        private ConnectionInfo GetConnectionInfo(string sessionId)
        {
            if (string.IsNullOrWhiteSpace(sessionId)) return null;
            if (connections.TryGetValue(sessionId, out var connectionInfo)) return connectionInfo;
            return null;
        }
        private void UnregisterClient(string sessionId) => connections.TryRemove(sessionId, out _);
        #endregion


        #region CleanUp and Dispose
        private void DisconnectAllClients()
        {
            Parallel.ForEach(connections.Values, connectionInfo =>
            {
                var clientChannel = connectionInfo?.Channel;
                if (clientChannel is not null)
                {
                    try
                    {
                        if (clientChannel.State == CommunicationState.Faulted) clientChannel.Abort();
                        else clientChannel.Close();
                    }
                    catch { clientChannel.Abort(); }
                }
            });
            connections.Clear();
        }
        private void CleanUp()
        {
            DisconnectAllClients();
            if (host is not null)
            {
                try
                {
                    if (host.State == CommunicationState.Faulted) host.Abort();
                    else host.Close();
                }
                catch { host.Abort(); }
                finally { host = null; }
            }
        }
        private volatile bool disposed;
        protected virtual void Dispose(bool disposing)
        {
            if (disposed) return;
            if (disposing)
            {
                lock (hostLock)
                {
                    if (disposed) return;
                    disposed = true;
                    if (interceptElement is not null) interceptElement.ConnectionChanged -= InterceptElement_ConnectionChanged;
                    CleanUp();
                }
            }
        }
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        #endregion


        private static IPEndPoint GetRemoteEndPoint(IDuplexSessionChannel channel)
        {
            const BindingFlags flags = BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public;

            var bufferConnection = channel.GetType()?.GetField("rawConnection", flags)?.GetValue(channel);
            var socketConnection = bufferConnection?.GetType()?.GetProperty("Connection", flags)?.GetValue(bufferConnection);
            return socketConnection?.GetType()?.GetProperty("RemoteIPEndPoint", flags)?.GetValue(socketConnection) as IPEndPoint;
        }
        private static IIdentity GetClientIdentity(IDuplexSessionChannel channel)
        {
            const BindingFlags flags = BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.DeclaredOnly;

            var channelType = typeof(IDuplexSessionChannel).Assembly.GetType("System.ServiceModel.Channels.TransportDuplexSessionChannel");
            var remoteSecuirityField = channelType?.GetField("remoteSecurity", flags);
            var remoteSecuirity = remoteSecuirityField?.GetValue(channel) as SecurityMessageProperty;
            return remoteSecuirity?.ServiceSecurityContext?.PrimaryIdentity;
        }
    }


    #region Service Helpers
    internal sealed class TcpHubServerMethodsProvider : IInstanceProvider
    {
        private readonly TcpHubServer server;
        public TcpHubServerMethodsProvider(TcpHubServer server) => this.server = server;

        public object GetInstance(InstanceContext instanceContext) => new TcpHubServer.TcpHubServerMethods(server);
        public object GetInstance(InstanceContext instanceContext, Message message) => GetInstance(instanceContext);
        public void ReleaseInstance(InstanceContext instanceContext, object instance) { if (instance is IDisposable disposable) disposable.Dispose(); }
    }

    internal sealed class TcpHubServiceBehavior : IServiceBehavior
    {
        private readonly TcpHubServer server;
        public TcpHubServiceBehavior(TcpHubServer server) => this.server = server;

        public void ApplyDispatchBehavior(ServiceDescription serviceDescription, ServiceHostBase serviceHostBase)
        {
            var provider = new TcpHubServerMethodsProvider(server);
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

    internal sealed class InterceptingBindingElement : BindingElement
    {
        public event Action<IChannel, ConnectionStatus> ConnectionChanged;

        public override IChannelListener<TChannel> BuildChannelListener<TChannel>(BindingContext context) => new InterceptingChannelListener<TChannel>(context.BuildInnerChannelListener<TChannel>(), ConnectionChanged);
        public override bool CanBuildChannelListener<TChannel>(BindingContext context) => context.CanBuildInnerChannelListener<TChannel>();
        public override BindingElement Clone() => new InterceptingBindingElement { ConnectionChanged = this.ConnectionChanged };
        public override T GetProperty<T>(BindingContext context) => context.GetInnerProperty<T>();
    }

    internal sealed class InterceptingChannelListener<TChannel> : ChannelListenerBase<TChannel> where TChannel : class, IChannel
    {
        private readonly IChannelListener<TChannel> innerListener;
        private readonly Action<IChannel, ConnectionStatus> connectionChanged;
        public InterceptingChannelListener(IChannelListener<TChannel> innerListener, Action<IChannel, ConnectionStatus> connectionChanged) { this.innerListener = innerListener; this.connectionChanged = connectionChanged; }

        private TChannel WrapAcceptedChannel(TChannel channel)
        {
            if (channel is not null)
            {
                channel.Opening += Channel_Opening;
                channel.Opened += Channel_Opened;
                channel.Faulted += Channel_Faulted;
                channel.Closing += Channel_Closing;
                channel.Closed += Channel_Closed;
            }
            return channel;

            void Channel_Faulted(object sender, EventArgs e)
            {
                channel.Opening -= Channel_Opening;
                channel.Opened -= Channel_Opened;
                channel.Faulted -= Channel_Faulted;
                channel.Closing -= Channel_Closing;
                channel.Closed -= Channel_Closed;

                connectionChanged?.Invoke(channel, ConnectionStatus.Faulted);
            }

            void Channel_Closed(object sender, EventArgs e)
            {
                channel.Opening -= Channel_Opening;
                channel.Opened -= Channel_Opened;
                channel.Faulted -= Channel_Faulted;
                channel.Closing -= Channel_Closing;
                channel.Closed -= Channel_Closed;

                connectionChanged?.Invoke(channel, ConnectionStatus.Disconnected);
            }

            void Channel_Opening(object sender, EventArgs e) => connectionChanged?.Invoke(channel, ConnectionStatus.Connecting);
            void Channel_Opened(object sender, EventArgs e) => connectionChanged?.Invoke(channel, ConnectionStatus.Connected);
            void Channel_Closing(object sender, EventArgs e) => connectionChanged?.Invoke(channel, ConnectionStatus.Disconnecting);

        }

        public override Uri Uri => innerListener.Uri;
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
    #endregion
}
