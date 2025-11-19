namespace Brighter.Transformers.Alibaba;

public class AlibabaConfiguration
{
    public static string TablestoreEndpoint { get; } = Environment.GetEnvironmentVariable("ALIBABA_TS_ENDPOINT") ?? string.Empty;
    public static string OssEndpoint { get; } = Environment.GetEnvironmentVariable("ALIBABA_OSS_ENDPOINT") ?? string.Empty;
    public static string AccessKey { get; } = Environment.GetEnvironmentVariable("ALIBABA_ACCESS_KEY") ?? string.Empty;
    public static string SecretKey { get; } = Environment.GetEnvironmentVariable("ALIBABA_SECRET_KEY") ?? string.Empty;
}