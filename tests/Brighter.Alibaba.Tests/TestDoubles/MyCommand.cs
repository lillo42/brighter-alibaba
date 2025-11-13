using Paramore.Brighter;

namespace Brighter.Transformers.Alibaba.TestDoubles;

public class MyCommand(): Command(Id.Random())
{
    public string Value { get; set; } = string.Empty;
}