
namespace XenonFileStore
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using System.Web;

    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;

    public class FileStore
    {
        private readonly string connectionString;

        public FileStore(string connectionString)
        {
            this.connectionString = connectionString;
        }

        public CloudBlobContainer GetContainer(string container, bool publicAccess)
        {
            CloudBlobContainer containerReference = CloudStorageAccount.Parse(connectionString).CreateCloudBlobClient().GetContainerReference(publicAccess ? container.ToString() + "-public" : container.ToString());
            if (containerReference.CreateIfNotExists() & publicAccess)
            {
                CloudBlobContainer cloudBlobContainer = containerReference;
                BlobContainerPermissions permissions =
                    new BlobContainerPermissions { PublicAccess = BlobContainerPublicAccessType.Blob };
                cloudBlobContainer.SetPermissions(permissions);
            }
            return containerReference;
        }

        public CloudBlockBlob GetBlob(string container, string filename, bool publicAccess)
        {
            return GetContainer(container, publicAccess).GetBlockBlobReference(filename);
        }

        public Task<Uri> PutFileAsync(string container, string filename, string filebody, bool publicAccess = false)
        {
            var c = this.GetContainer(container, publicAccess);
            return this.PutFileAsync(c, filename, filebody);
        }

        public Task<Uri> PutFileAsync(string container, string filename, Stream filebody, bool publicAccess = false)
        {
            var c = this.GetContainer(container, publicAccess);
            return this.PutFileAsync(c, filename, filebody);
        }

        public Task<Uri> PutFileAsync(string container, string filename, string filepath, AccessCondition accessCondition, bool publicAccess = false)
        {
            var c = this.GetContainer(container, publicAccess);
            return this.PutFileAsync(c, filename, filepath, accessCondition);
        }

        public Task<string> GetFileAsStringAsync(string container, string filename, Encoding encoding, bool publicAccess = false)
        {
            var c = this.GetContainer(container, publicAccess);
            return this.GetFileAsStringAsync(c, filename, encoding);
        }

        public Task<FileItem> GetFileAsync(string container, string filename, Stream target, bool publicAccess = false)
        {
            var c = this.GetContainer(container, publicAccess);
            return this.GetFileAsync(c, filename, target);
        }

        public Task<bool> DeleteFileAsync(string container, string filename, bool publicAccess = false)
        {
            var c = this.GetContainer(container, publicAccess);
            return this.DeleteFileAsync(c, filename);
        }

        public IEnumerable<FileItem> ListFiles(string container, string prefix = null, bool publicAccess = false)
        {
            var c = this.GetContainer(container, publicAccess);
            return this.ListFiles(c, prefix);
        }

        public Uri GetUrl(string container, string filename)
        {
            var c = this.GetContainer(container, true);
            return this.GetUrl(c, filename);
        }

        public Task<bool> ExistsAsync(string container, string filename, bool publicAccess = false)
        {
            var c = this.GetContainer(container, publicAccess);
            return this.ExistsAsync(c, filename);
        }

        public Task DeleteContainerAsync(string container, bool publicAccess = false)
        {
            return GetContainer(container, publicAccess).DeleteIfExistsAsync();
        }

        #region GuidMethods

        public Task<Uri> PutFileAsync(Guid containerGuid, string filename, string filebody, bool publicAccess = false)
        {
            var c = this.GetContainer(containerGuid, publicAccess);
            return this.PutFileAsync(c, filename, filebody);
        }

        public Task<Uri> PutFileAsync(Guid containerGuid, string filename, Stream filebody, bool publicAccess = false)
        {
            var c = this.GetContainer(containerGuid, publicAccess);
            return this.PutFileAsync(c, filename, filebody);
        }

        public Task<Uri> PutFileAsync(Guid containerGuid, string filename, string filepath, AccessCondition accessCondition, bool publicAccess)
        {
            var c = this.GetContainer(containerGuid, publicAccess);
            return this.PutFileAsync(c, filename, filepath, accessCondition);
        }

        public Task<string> GetFileAsStringAsync(Guid containerGuid, string filename, Encoding encoding, bool publicAccess = false)
        {
            var c = this.GetContainer(containerGuid, publicAccess);
            return this.GetFileAsStringAsync(c, filename, encoding);
        }

        public Task<FileItem> GetFileAsync(Guid containerGuid, string filename, Stream target, bool publicAccess = false)
        {
            var c = this.GetContainer(containerGuid, publicAccess);
            return this.GetFileAsync(c, filename, target);
        }

        public Task<bool> DeleteFileAsync(Guid containerGuid, string filename, bool publicAccess = false)
        {
            var c = this.GetContainer(containerGuid, publicAccess);
            return this.DeleteFileAsync(c, filename);
        }

        public IEnumerable<FileItem> ListFiles(Guid containerGuid, string prefix = null, bool publicAccess = false)
        {
            var c = this.GetContainer(containerGuid, publicAccess);
            return this.ListFiles(c, prefix);
        }

        public Uri GetUrl(Guid containerGuid, string filename)
        {
            var c = this.GetContainer(containerGuid, true);
            return this.GetUrl(c, filename);
        }

        public Task<bool> Exists(Guid containerGuid, string filename, bool publicAccess = false)
        {
            var c = this.GetContainer(containerGuid, publicAccess);
            return this.ExistsAsync(c, filename);
        }

        public Task DeleteContainerAsync(Guid containerGuid, bool publicAccess)
        {
            return GetContainer(containerGuid, publicAccess).DeleteIfExistsAsync();
        }

        private CloudBlobContainer GetContainer(Guid containerGuid, bool publicAccess)
        {
            return this.GetContainer(containerGuid.ToString(), publicAccess);
        }

        #endregion

        private StreamReader CreateReader(Stream stream, Encoding encoding)
        {
            return new StreamReader(stream, encoding, true, 1024, true);
        }

        private CloudBlockBlob GetBlob(CloudBlobContainer container, string filename)
        {
            return container.GetBlockBlobReference(filename);
        }

        private async Task<Uri> PutFileAsync(CloudBlobContainer container, string filename, string filebody)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(filebody);
            using (var stream = new MemoryStream(bytes))
            {
                var ret = await PutFileAsync(container, filename, stream).ConfigureAwait(false);
                return ret;
            }
        }

        private async Task<Uri> PutFileAsync(CloudBlobContainer container, string filename, Stream filebody)
        {
            CloudBlockBlob blockBlob = GetBlob(container, filename);
            await blockBlob.UploadFromStreamAsync(filebody).ConfigureAwait(false);
            blockBlob.Properties.ContentType = MimeMapping.GetMimeMapping(filename);
            blockBlob.SetProperties();
            return blockBlob.Uri;
        }

        private async Task<Uri> PutFileAsync(CloudBlobContainer container, string filename, string filepath, AccessCondition accessCondition)
        {
            CloudBlockBlob blob = GetBlob(container, filename);
            await blob
                .UploadFromFileAsync(filepath, accessCondition, default(BlobRequestOptions), default(OperationContext))
                .ConfigureAwait(false);
            blob.Properties.ContentType = MimeMapping.GetMimeMapping(filename);
            blob.SetProperties();
            return blob.Uri;
        }

        private async Task<string> GetFileAsStringAsync(CloudBlobContainer container, string filename, Encoding encoding)
        {
            using (MemoryStream memoryStream = new MemoryStream())
            {
                await GetFileAsync(container, filename, memoryStream).ConfigureAwait(false);
                memoryStream.Position = 0L;

                using (StreamReader streamReader = this.CreateReader(memoryStream, encoding))
                {
                    string ret = await streamReader.ReadToEndAsync().ConfigureAwait(false);
                    return ret;
                }
            }
        }

        private async Task<FileItem> GetFileAsync(CloudBlobContainer container, string filename, Stream target)
        {
            CloudBlockBlob blob = GetBlob(container, filename);
            await blob.FetchAttributesAsync().ConfigureAwait(false);

            FileItem fileItem = new FileItem
            {
                Uri = blob.Uri,
                ContentType = blob.Properties.ContentType,
                LastModified = blob.Properties.LastModified,
                Length = blob.Properties.Length,
                Name = blob.Name,
                Container = container.Name,
            };

            await blob.DownloadToStreamAsync(target).ConfigureAwait(false);
            return fileItem;
        }

        private async Task<bool> DeleteFileAsync(CloudBlobContainer container, string filename)
        {
            CloudBlockBlob blob = GetBlob(container, filename);
            bool exists = await blob.ExistsAsync().ConfigureAwait(false);
            if (!exists)
            {
                return false;
            }

            await blob.DeleteAsync().ConfigureAwait(false);
            return true;
        }

        private IEnumerable<FileItem> ListFiles(CloudBlobContainer container, string prefix = null)
        {
            var blobs = container.ListBlobs(prefix, useFlatBlobListing: true);
            var cblobs = blobs.Select(b => b as CloudBlockBlob);
            return cblobs.Select(c => new FileItem
            {
                Uri = c.Uri,
                Container = container.Name,
                ContentType = c.Properties.ContentType,
                Name = c.Name,
                LastModified = c.Properties.LastModified,
                Length = c.Properties.Length,
            }).OrderBy(f => f.Name);
        }

        public Uri GetUrl(CloudBlobContainer container, string filename)
        {
            return new Uri(container.Uri.ToString() + "/" + filename);
        }

        public Task<bool> ExistsAsync(CloudBlobContainer container, string filename)
        {
            return GetBlob(container, filename).ExistsAsync();
        }
    }
}
