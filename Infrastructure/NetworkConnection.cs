using System.Net;
using System.Runtime.InteropServices;

namespace Infrastructure.SmbSyncService;

public class NetworkConnection : IDisposable
{
    private readonly string _networkName;

    public NetworkConnection(string networkName, NetworkCredential credentials)
    {
        _networkName = networkName;

        var netResource = new NetResource
        {
            Scope = ResourceScope.GlobalNetwork,
            ResourceType = ResourceType.Disk,
            DisplayType = ResourceDisplayType.Share,
            RemoteName = networkName
        };

        var result = WNetAddConnection2(
            netResource,
            credentials.Password,
            credentials.UserName,
            0);

        if (result != 0)
        {
            throw new Exception($"Error connecting to remote share. Error code: {result}");
        }
    }

    public void Dispose()
    {
        WNetCancelConnection2(_networkName, 0, true);
    }

    [DllImport("mpr.dll", CharSet = CharSet.Unicode)]
    private static extern int WNetAddConnection2(NetResource netResource, string password, string username, int flags);

    [DllImport("mpr.dll", CharSet = CharSet.Unicode)]
    private static extern int WNetCancelConnection2(string name, int flags, bool force);

    [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
    private class NetResource
    {
        public ResourceScope Scope;
        public ResourceType ResourceType;
        public ResourceDisplayType DisplayType;
        public int Usage;
        public string? LocalName;
        public string RemoteName = string.Empty;
        public string? Comment;
        public string? Provider;
    }

    private enum ResourceScope : int { GlobalNetwork = 2 }
    private enum ResourceType : int { Disk = 1 }
    private enum ResourceDisplayType : int { Share = 3 }
}