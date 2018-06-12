using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace XenonFileStore
{
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
            if (containerReference.CreateIfNotExists((BlobRequestOptions)null, (OperationContext)null) & publicAccess)
            {
                CloudBlobContainer cloudBlobContainer = containerReference;
                BlobContainerPermissions permissions =
                    new BlobContainerPermissions { PublicAccess = BlobContainerPublicAccessType.Blob };
                cloudBlobContainer.SetPermissions(permissions, null, null, null);
            }
            return containerReference;
        }

        public CloudBlockBlob GetBlob(string container, string filename, bool publicAcess)
        {
            return GetContainer(container, publicAcess).GetBlockBlobReference(filename);
        }

        public async Task<Uri> PutFile(string container, string filename, string filebody, bool publicAccess)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(filebody);
            Uri uri = await PutFile(container, filename, (Stream)new MemoryStream(bytes), publicAccess);
            return uri;
        }

        public async Task<Uri> PutFile(string container, string filename, Stream filebody)
        {
            Uri uri = await PutFile(container, filename, filebody, false);
            return uri;
        }

        public async Task<Uri> PutFile(string container, string filename, Stream filebody, bool publicAcess)
        {
            CloudBlockBlob blockBlob = GetBlob(container, filename, publicAcess);
            await blockBlob.UploadFromStreamAsync(filebody);
            blockBlob.Properties.ContentType = MimeMapping.GetMimeMapping(filename);
            blockBlob.SetProperties((AccessCondition)null, (BlobRequestOptions)null, (OperationContext)null);
            return blockBlob.Uri;
        }

        public Uri PutFile(string container, string filename, string filepath, AccessCondition accessCondition, bool publicAcess)
        {
            CloudBlockBlob blob = GetBlob(container, filename, publicAcess);
            blob.UploadFromFile(filepath, accessCondition, (BlobRequestOptions)null, (OperationContext)null);
            blob.Properties.ContentType = MimeMapping.GetMimeMapping(filename);
            blob.SetProperties((AccessCondition)null, (BlobRequestOptions)null, (OperationContext)null);
            return blob.Uri;
        }

        public string GetFileAsString(string container, string filename, bool publicAccess)
        {
            MemoryStream memoryStream = new MemoryStream();
            GetFile(container, filename, (Stream)memoryStream);
            memoryStream.Position = 0L;
            StreamReader streamReader = new StreamReader((Stream)memoryStream);
            string end = streamReader.ReadToEnd();
            streamReader.Close();
            memoryStream.Close();
            return end;
        }

        public FileItem GetFile(string container, string filename, Stream target)
        {
            return GetFile(container, filename, target, false);
        }

        public FileItem GetFile(string container, string filename, Stream target, bool publicAccess)
        {
            CloudBlockBlob blob = GetBlob(container, filename, publicAccess);
            blob.FetchAttributes((AccessCondition)null, (BlobRequestOptions)null, (OperationContext)null);
            FileItem fileItem = new FileItem()
            {
                Uri = blob.Uri,
                ContentType = blob.Properties.ContentType,
                LastModified = blob.Properties.LastModified,
                Length = blob.Properties.Length,
                Name = blob.Name,
                Container = container
            };
            blob.DownloadToStream(target, null, null, null);
            return fileItem;
        }

        public bool DeleteFile(string container, string filename)
        {
            return DeleteFile(container, filename, false);
        }

        public bool DeleteFile(string container, string filename, bool publicAccess)
        {
            CloudBlockBlob blob = GetBlob(container, filename, publicAccess);
            if (!blob.Exists((BlobRequestOptions)null, (OperationContext)null))
                return false;
            blob.Delete(DeleteSnapshotsOption.None, (AccessCondition)null, (BlobRequestOptions)null, (OperationContext)null);
            return true;
        }

        public List<FileItem> ListFiles(string container)
        {
            return ListFiles(container, (string)null, false);
        }

        public List<FileItem> ListFiles(string container, bool publicAccess)
        {
            return ListFiles(container, (string)null, publicAccess);
        }

        public List<FileItem> ListFiles(string container, string prefix)
        {
            return ListFiles(container, prefix, false);
        }

        public List<FileItem> ListFiles(string container, string prefix, bool publicAccess)
        {
            var cont = GetContainer(container, publicAccess);
            var blobs = cont.ListBlobs(prefix, useFlatBlobListing: true);
            var cblobs = blobs.Select(b => b as CloudBlockBlob);
            return cblobs.Select(c => new FileItem()
            {
                Uri = c.Uri,
                Container = container,
                ContentType = c.Properties.ContentType,
                Name = c.Name,
                LastModified = c.Properties.LastModified,
                Length = c.Properties.Length
            }).OrderBy<FileItem, string>((Func<FileItem, string>)(f => f.Name)).ToList<FileItem>();
        }

        public Uri GetUrl(string container, string filename)
        {
            return new Uri(GetContainer(container, true).Uri.ToString() + "/" + filename);
        }

        public bool Exists(string container, string filename, bool publicAccess)
        {
            return GetBlob(container, filename, publicAccess).Exists((BlobRequestOptions)null, (OperationContext)null);
        }

        public void DeleteContainer(string container, bool publicAccess)
        {
            GetContainer(container, publicAccess).DeleteIfExists((AccessCondition)null, (BlobRequestOptions)null, (OperationContext)null);
        }

        #region GuidMethods

        private CloudBlobContainer GetContainer(Guid containerGuid, bool publicAccess)
        {
            CloudBlobContainer containerReference = CloudStorageAccount.Parse(connectionString).CreateCloudBlobClient().GetContainerReference(publicAccess ? containerGuid.ToString() + "-public" : containerGuid.ToString());
            if (containerReference.CreateIfNotExists((BlobRequestOptions)null, (OperationContext)null) & publicAccess)
            {
                CloudBlobContainer cloudBlobContainer = containerReference;
                BlobContainerPermissions permissions =
                    new BlobContainerPermissions { PublicAccess = BlobContainerPublicAccessType.Blob };
                cloudBlobContainer.SetPermissions(permissions, null);
            }
            return containerReference;
        }

        private CloudBlockBlob GetBlob(Guid containerGuid, string filename, bool publicAcess)
        {
            return GetContainer(containerGuid, publicAcess).GetBlockBlobReference(filename);
        }

        public async Task<Uri> PutFile(Guid containerGuid, string filename, string filebody, bool publicAccess)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(filebody);
            Uri uri = await PutFile(containerGuid, filename, (Stream)new MemoryStream(bytes), publicAccess);
            return uri;
        }

        public async Task<Uri> PutFile(Guid containerGuid, string filename, Stream filebody)
        {
            Uri uri = await PutFile(containerGuid, filename, filebody, false);
            return uri;
        }

        public async Task<Uri> PutFile(Guid containerGuid, string filename, Stream filebody, bool publicAcess)
        {
            CloudBlockBlob blockBlob = GetBlob(containerGuid, filename, publicAcess);
            await blockBlob.UploadFromStreamAsync(filebody);
            blockBlob.Properties.ContentType = MimeMapping.GetMimeMapping(filename);
            blockBlob.SetProperties((AccessCondition)null, (BlobRequestOptions)null, (OperationContext)null);
            return blockBlob.Uri;
        }

        public Uri PutFile(Guid containerGuid, string filename, string filepath, AccessCondition accessCondition, bool publicAcess)
        {
            CloudBlockBlob blob = GetBlob(containerGuid, filename, publicAcess);
            blob.UploadFromFile(filepath, accessCondition, (BlobRequestOptions)null, (OperationContext)null);
            blob.Properties.ContentType = MimeMapping.GetMimeMapping(filename);
            blob.SetProperties((AccessCondition)null, (BlobRequestOptions)null, (OperationContext)null);
            return blob.Uri;
        }

        public string GetFileAsString(Guid containerGuid, string filename, bool publicAccess)
        {
            MemoryStream memoryStream = new MemoryStream();
            GetFile(containerGuid, filename, (Stream)memoryStream);
            memoryStream.Position = 0L;
            StreamReader streamReader = new StreamReader((Stream)memoryStream);
            string end = streamReader.ReadToEnd();
            streamReader.Close();
            memoryStream.Close();
            return end;
        }

        public FileItem GetFile(Guid containerGuid, string filename, Stream target)
        {
            return GetFile(containerGuid, filename, target, false);
        }

        public FileItem GetFile(Guid containerGuid, string filename, Stream target, bool publicAccess)
        {
            CloudBlockBlob blob = GetBlob(containerGuid, filename, publicAccess);
            blob.FetchAttributes((AccessCondition)null, (BlobRequestOptions)null, (OperationContext)null);
            FileItem fileItem = new FileItem()
            {
                Uri = blob.Uri,
                ContentType = blob.Properties.ContentType,
                LastModified = blob.Properties.LastModified,
                Length = blob.Properties.Length,
                Name = blob.Name
            };
            blob.DownloadToStream(target, (AccessCondition)null, (BlobRequestOptions)null, (OperationContext)null);
            return fileItem;
        }

        public bool DeleteFile(Guid containerGuid, string filename)
        {
            return DeleteFile(containerGuid, filename, false);
        }

        public bool DeleteFile(Guid containerGuid, string filename, bool publicAccess)
        {
            CloudBlockBlob blob = GetBlob(containerGuid, filename, publicAccess);
            if (!blob.Exists((BlobRequestOptions)null, (OperationContext)null))
                return false;
            blob.Delete(DeleteSnapshotsOption.None, (AccessCondition)null, (BlobRequestOptions)null, (OperationContext)null);
            return true;
        }

        public List<FileItem> ListFiles(Guid containerGuid)
        {
            return ListFiles(containerGuid, (string)null, false);
        }

        public List<FileItem> ListFiles(Guid containerGuid, bool publicAccess)
        {
            return ListFiles(containerGuid, (string)null, publicAccess);
        }

        public List<FileItem> ListFiles(Guid containerGuid, string prefix)
        {
            return ListFiles(containerGuid, prefix, false);
        }

        public List<FileItem> ListFiles(Guid containerGuid, string prefix, bool publicAccess)
        {
            return GetContainer(containerGuid, publicAccess).ListBlobs(prefix, publicAccess, BlobListingDetails.None, (BlobRequestOptions)null, (OperationContext)null).Select<IListBlobItem, CloudBlockBlob>((Func<IListBlobItem, CloudBlockBlob>)(b => b as CloudBlockBlob)).Select<CloudBlockBlob, FileItem>((Func<CloudBlockBlob, FileItem>)(c => new FileItem()
            {
                Uri = c.Uri,
                Container = containerGuid.ToString(),
                ContentType = c.Properties.ContentType,
                Name = c.Name,
                LastModified = c.Properties.LastModified,
                Length = c.Properties.Length
            })).OrderBy<FileItem, string>((Func<FileItem, string>)(f => f.Name)).ToList<FileItem>();
        }

        public Uri GetUrl(Guid containerGuid, string filename)
        {
            return new Uri(GetContainer(containerGuid, true).Uri.ToString() + "/" + filename);
        }

        public bool Exists(Guid containerGuid, string filename, bool publicAccess)
        {
            return GetBlob(containerGuid, filename, publicAccess).Exists((BlobRequestOptions)null, (OperationContext)null);
        }

        public void DeleteContainer(Guid containerGuid, bool publicAccess)
        {
            GetContainer(containerGuid, publicAccess).DeleteIfExists((AccessCondition)null, (BlobRequestOptions)null, (OperationContext)null);
        }

        #endregion
    }

    
}
