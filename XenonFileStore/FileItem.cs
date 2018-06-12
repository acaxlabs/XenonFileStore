using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace XenonFileStore
{
    public class FileItem
    {
        public string Name { get; set; }

        public string Container { get; set; }

        public Uri Uri { get; set; }

        public DateTimeOffset? LastModified { get; set; }

        public long Length { get; set; }

        public string ContentType { get; set; }
    }
}
