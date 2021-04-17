using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace CCFPerformanceTester.Helper
{
    public class RequestFormat
    {
        public HttpMethod Method { get; set; }
        public string Path { get; set; }
        public MessageContent Content { get; set; }
    }
}
