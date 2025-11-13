namespace Brighter.Transformers.Alibaba;

public class AlibabaConfiguration
{
    public static string Endpoint { get; } = Environment.GetEnvironmentVariable("ALIBABA_ENDPOINT") ?? string.Empty;
    public static string AccessKey { get; } = Environment.GetEnvironmentVariable("ALIBABA_ACCESS_KEY") ?? string.Empty;
    public static string SecretKey { get; } = Environment.GetEnvironmentVariable("ALIBABA_SECRET_KEY") ?? string.Empty;
}