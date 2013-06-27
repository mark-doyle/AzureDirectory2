//    License: Microsoft Public License (Ms-PL) 
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using IndexFileNameFilter = Lucene.Net.Index.IndexFileNameFilter;
using Lucene.Net;
using Lucene.Net.Store;
using System.Collections.Specialized;
using System.Diagnostics;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters;
using System.Runtime.Serialization.Formatters.Binary;
using Microsoft.WindowsAzure;
using System.Configuration;
using System.Xml.Serialization;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage;
//using Microsoft.WindowsAzure.StorageClient;

namespace Lucene.Net.Store.Azure
{
    public class AzureDirectory : Directory
    {
        #region Declarations

        private string _catalog;
        private CloudBlobClient _blobClient;
        private CloudBlobContainer _blobContainer;
        private Directory _cacheDirectory;
        private Dictionary<string, AzureLock> _locks;

        #endregion // Declarations

        #region Constructors

        public AzureDirectory(CloudStorageAccount storageAccount) :
            this(storageAccount, null, null)
        {
        }

        /// <summary>
        /// Create AzureDirectory
        /// </summary>
        /// <param name="storageAccount">staorage account to use</param>
        /// <param name="catalog">name of catalog (folder in blob storage)</param>
        /// <remarks>Default local cache is to use file system in user/appdata/AzureDirectory/Catalog</remarks>
        public AzureDirectory(
            CloudStorageAccount storageAccount,
            string catalog)
            : this(storageAccount, catalog, null)
        {
        }

        /// <summary>
        /// Create an AzureDirectory
        /// </summary>
        /// <param name="storageAccount">storage account to use</param>
        /// <param name="catalog">name of catalog (folder in blob storage)</param>
        /// <param name="cacheDirectory">local Directory object to use for local cache</param>
        public AzureDirectory(
            CloudStorageAccount storageAccount,
            string catalog,
            Directory cacheDirectory)
        {
            _locks = new Dictionary<string, AzureLock>();

            if (storageAccount == null)
                throw new ArgumentNullException("storageAccount");

            if (string.IsNullOrEmpty(catalog))
                _catalog = "lucene";
            else
                _catalog = catalog.ToLower();

            _blobClient = storageAccount.CreateCloudBlobClient();
            _initCacheDirectory(cacheDirectory);
        }

        #endregion // Constructors

        #region Private Methods

        private void _initCacheDirectory(Directory cacheDirectory)
        {
#if COMPRESSBLOBS
            CompressBlobs = true;
#endif
            if (cacheDirectory != null)
            {
                // save it off
                _cacheDirectory = cacheDirectory;
            }
            else
            {
                // Creates parent directory for all AzureDirectory indexes
                string cachePath = System.IO.Path.Combine(Environment.ExpandEnvironmentVariables("%temp%"), "AzureDirectory");
                System.IO.DirectoryInfo azureDir = new System.IO.DirectoryInfo(cachePath);
                if (!azureDir.Exists)
                    azureDir.Create();

                // Creates child directory for specific index
                string catalogPath = System.IO.Path.Combine(cachePath, _catalog);
                System.IO.DirectoryInfo catalogDir = new System.IO.DirectoryInfo(catalogPath);
                if (!catalogDir.Exists)
                    catalogDir.Create();

                _cacheDirectory = FSDirectory.Open(catalogPath);
            }

            this.CreateContainer();
        }

        #endregion // Private Methods

        #region Public Methods

        public void CreateContainer()
        {
            if (this._blobContainer == null)
            {
                _blobContainer = _blobClient.GetContainerReference(_catalog);

                // create it if it does not exist
                _blobContainer.CreateIfNotExists();
            }
        }

        public void ClearCache()
        {
            foreach (string file in _cacheDirectory.ListAll())
            {
                _cacheDirectory.DeleteFile(file);
            }
        }

        #region DIRECTORY METHODS

        /// <summary>Returns an array of strings, one for each file in the directory. </summary>
        public override System.String[] ListAll()
        {
            var results = from blob in BlobContainer.ListBlobs()
                          select blob.Uri.AbsolutePath.Substring(blob.Uri.AbsolutePath.LastIndexOf('/') + 1);
            return results.ToArray<string>();
        }

        /// <summary>Returns true if a file with the given name exists. </summary>
        public override bool FileExists(System.String name)
        {
            // this always comes from the server
            try
            {
                var blob = BlobContainer.GetBlockBlobReference(name);
                blob.FetchAttributes();
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        /// <summary>Returns the time the named file was last modified. </summary>
        public override long FileModified(System.String name)
        {
            // this always has to come from the server
            try
            {
                var blob = BlobContainer.GetBlockBlobReference(name);
                blob.FetchAttributes();
                return blob.Properties.LastModified.Value.UtcDateTime.ToFileTimeUtc();
            }
            catch
            {
                return 0;
            }
        }

        /// <summary>Set the modified time of an existing file to now. </summary>
        public override void TouchFile(System.String name)
        {
            _cacheDirectory.TouchFile(name);
        }

        /// <summary>Removes an existing file in the directory. </summary>
        public override void DeleteFile(System.String name)
        {
            var blob = BlobContainer.GetBlockBlobReference(name);
            blob.DeleteIfExists();
            Debug.WriteLine(String.Format("DELETE {0}/{1}", _blobContainer.Uri.ToString(), name));

            if (_cacheDirectory.FileExists(name + ".blob"))
                _cacheDirectory.DeleteFile(name + ".blob");

            if (_cacheDirectory.FileExists(name))
                _cacheDirectory.DeleteFile(name);
        }

        /// <summary>Returns the length of a file in the directory. </summary>
        public override long FileLength(System.String name)
        {
            try
            {
                var blob = BlobContainer.GetBlockBlobReference(name);
                blob.FetchAttributes();

                // index files may be compressed so the actual length is stored in metatdata
                long blobLength;
                if (long.TryParse(blob.Metadata["CachedLength"], out blobLength))
                    return blobLength;
                else
                    return blob.Properties.Length; // fall back to actual blob size
            }
            catch
            {
                return 0;
            }
        }

        /// <summary>Creates a new, empty file in the directory with the given name.
        /// Returns a stream writing this file. 
        /// </summary>
        public override IndexOutput CreateOutput(System.String name)
        {
            var blob = BlobContainer.GetBlockBlobReference(name);
            return new AzureIndexOutput(this, blob);
        }

        /// <summary>Returns a stream reading an existing file. </summary>
        public override IndexInput OpenInput(System.String name)
        {
            try
            {
                var blob = BlobContainer.GetBlockBlobReference(name);
                blob.FetchAttributes();
                AzureIndexInput input = new AzureIndexInput(this, blob);
                return input;
            }
            catch (Exception err)
            {
                throw new System.IO.FileNotFoundException(name, err);
            }
        }


        /// <summary>Construct a {@link Lock}.</summary>
        /// <param name="name">the name of the lock file
        /// </param>
        public override Lock MakeLock(System.String name)
        {
            lock (_locks)
            {
                if (!_locks.ContainsKey(name))
                    _locks.Add(name, new AzureLock(name, this));
                return _locks[name];
            }
        }

        public override void ClearLock(string name)
        {
            lock (_locks)
            {
                if (_locks.ContainsKey(name))
                {
                    _locks[name].BreakLock();
                }
            }
            _cacheDirectory.ClearLock(name);
        }

        /// <summary>Closes the store. </summary>
        protected override void Dispose(bool disposing)
        {
            _blobContainer = null;
            _blobClient = null;
        }

        #endregion // DIRECTORY METHODS

        #region Azure specific methods

#if COMPRESSBLOBS
        public virtual bool ShouldCompressFile(string path)
        {
            if (!CompressBlobs)
                return false;

            string ext = System.IO.Path.GetExtension(path);
            switch (ext)
            {
                case ".cfs":
                case ".fdt":
                case ".fdx":
                case ".frq":
                case ".tis":
                case ".tii":
                case ".nrm":
                case ".tvx":
                case ".tvd":
                case ".tvf":
                case ".prx":
                    return true;
                default:
                    return false;
            };
        }
#endif
        public StreamInput OpenCachedInputAsStream(string name)
        {
            return new StreamInput(CacheDirectory.OpenInput(name));
        }

        public StreamOutput CreateCachedOutputAsStream(string name)
        {
            return new StreamOutput(CacheDirectory.CreateOutput(name));
        }

        #endregion // Azure specific methods

        #endregion // Public Methods

        #region Public Properties

        public CloudBlobContainer BlobContainer
        {
            get
            {
                // Create container, just in case reference was lost
                this.CreateContainer();

                return _blobContainer;
            }
        }

#if COMPRESSBLOBS
        public bool CompressBlobs
        {
            get;
            set;
        }
#endif

        public Directory CacheDirectory
        {
            get { return _cacheDirectory; }
            set { _cacheDirectory = value; }
        }

        #endregion // Public Properties

    }

}
