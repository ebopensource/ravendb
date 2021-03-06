﻿// -----------------------------------------------------------------------
//  <copyright file="FileActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;

using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.FileSystem.Notifications;
using Raven.Abstractions.Logging;
using Raven.Abstractions.MEF;
using Raven.Abstractions.Util.Encryptors;
using Raven.Abstractions.Util.Streams;
using Raven.Database.Extensions;
using Raven.Database.FileSystem.Extensions;
using Raven.Database.FileSystem.Plugins;
using Raven.Database.FileSystem.Storage;
using Raven.Database.FileSystem.Storage.Exceptions;
using Raven.Database.FileSystem.Util;
using Raven.Json.Linq;

namespace Raven.Database.FileSystem.Actions
{
	public class FileActions : ActionsBase
	{
		private readonly ConcurrentDictionary<string, Task> deleteFileTasks = new ConcurrentDictionary<string, Task>();
		private readonly ConcurrentDictionary<string, Task> renameFileTasks = new ConcurrentDictionary<string, Task>();
		private readonly ConcurrentDictionary<string, FileHeader> uploadingFiles = new ConcurrentDictionary<string, FileHeader>();

		private readonly IObservable<long> timer = Observable.Interval(TimeSpan.FromMinutes(15));

		public FileActions(RavenFileSystem fileSystem, ILog log)
			: base(fileSystem, log)
		{
			InitializeTimer();
		}

		private void InitializeTimer()
		{
			timer.Subscribe(tick =>
			{
				ResumeFileRenamingAsync();
				CleanupDeletedFilesAsync();
			});
		}

		public async Task PutAsync(string name, Etag etag, RavenJObject metadata, Func<Task<Stream>> streamAsync, PutOperationOptions options)
		{
			try
			{
				FileSystem.MetricsCounters.FilesPerSecond.Mark();

				name = FileHeader.Canonize(name);

				if (options.PreserveTimestamps)
				{
					if (!metadata.ContainsKey(Constants.RavenCreationDate))
					{
						if (metadata.ContainsKey(Constants.CreationDate))
							metadata[Constants.RavenCreationDate] = metadata[Constants.CreationDate];
						else
							throw new InvalidOperationException("Preserve Timestamps requires that the client includes the Raven-Creation-Date header.");
					}

					Historian.UpdateLastModified(metadata, options.LastModified.HasValue ? options.LastModified.Value : DateTimeOffset.UtcNow);
				}
				else
				{
					metadata[Constants.RavenCreationDate] = DateTimeOffset.UtcNow;

					Historian.UpdateLastModified(metadata);
				}

				// TODO: To keep current filesystems working. We should remove when adding a new migration. 
				metadata[Constants.CreationDate] = metadata[Constants.RavenCreationDate].Value<DateTimeOffset>().ToString("yyyy-MM-ddTHH:mm:ss.fffffffZ", CultureInfo.InvariantCulture);

				Historian.Update(name, metadata);

				SynchronizationTask.Cancel(name);

				long? size = -1;
				Storage.Batch(accessor =>
				{
					AssertPutOperationNotVetoed(name, metadata);
					AssertFileIsNotBeingSynced(name, accessor);

					var contentLength = options.ContentLength;
					var contentSize = options.ContentSize;

					if (contentLength == 0 || contentSize.HasValue == false)
					{
						size = contentLength;
						if (options.TransferEncodingChunked)
							size = null;
					}
					else
					{
						size = contentSize;
					}

					FileSystem.PutTriggers.Apply(trigger => trigger.OnPut(name, metadata));

					using (FileSystem.DisableAllTriggersForCurrentThread())
					{
						IndicateFileToDelete(name, etag);
					}

					var putResult = accessor.PutFile(name, size, metadata);

					FileSystem.PutTriggers.Apply(trigger => trigger.AfterPut(name, size, metadata));

					Search.Index(name, metadata, putResult.Etag);
				});

				Log.Debug("Inserted a new file '{0}' with ETag {1}", name, metadata.Value<string>(Constants.MetadataEtagField));

				using (var contentStream = await streamAsync())
				using (var readFileToDatabase = new ReadFileToDatabase(BufferPool, Storage, FileSystem.PutTriggers, contentStream, name, metadata))
				{
					await readFileToDatabase.Execute();

					if (readFileToDatabase.TotalSizeRead != size)
					{
						IndicateFileToDelete(name, null);
						throw new HttpResponseException(HttpStatusCode.BadRequest);
					}

					if (options.PreserveTimestamps == false)
						Historian.UpdateLastModified(metadata); // update with the final file size.

					Log.Debug("File '{0}' was uploaded. Starting to update file metadata and indexes", name);

					metadata["Content-MD5"] = readFileToDatabase.FileHash;

					FileOperationResult updateMetadata = null;
					Storage.Batch(accessor => updateMetadata = accessor.UpdateFileMetadata(name, metadata, null));

					int totalSizeRead = readFileToDatabase.TotalSizeRead;
					metadata["Content-Length"] = totalSizeRead.ToString(CultureInfo.InvariantCulture);

					Search.Index(name, metadata, updateMetadata.Etag);
					Publisher.Publish(new FileChangeNotification { Action = FileChangeAction.Add, File = FilePathTools.Cannoicalise(name) });

					Log.Debug("Updates of '{0}' metadata and indexes were finished. New file ETag is {1}", name, metadata.Value<string>(Constants.MetadataEtagField));

					FileSystem.Synchronization.StartSynchronizeDestinationsInBackground();
				}
			}
			catch (Exception ex)
			{
				if (options.UploadId.HasValue)
					Publisher.Publish(new CancellationNotification { UploadId = options.UploadId.Value, File = name });

				Log.WarnException(string.Format("Failed to upload a file '{0}'", name), ex);

				throw;
			}
		}

		private void AssertPutOperationNotVetoed(string name, RavenJObject headers)
		{
			var vetoResult = FileSystem.PutTriggers
				.Select(trigger => new { Trigger = trigger, VetoResult = trigger.AllowPut(name, headers) })
				.FirstOrDefault(x => x.VetoResult.IsAllowed == false);
			if (vetoResult != null)
			{
				throw new OperationVetoedException("PUT vetoed on file " + name + " by " + vetoResult.Trigger + " because: " + vetoResult.VetoResult.Reason);
			}
		}

		private void AssertDeleteOperationNotVetoed(string name)
		{
			var vetoResult = FileSystem.DeleteTriggers
				.Select(trigger => new { Trigger = trigger, VetoResult = trigger.AllowDelete(name) })
				.FirstOrDefault(x => x.VetoResult.IsAllowed == false);
			if (vetoResult != null)
			{
				throw new OperationVetoedException("DELETE vetoed on file " + name + " by " + vetoResult.Trigger + " because: " + vetoResult.VetoResult.Reason);
			}
		}

		private void AssertFileIsNotBeingSynced(string fileName, IStorageActionsAccessor accessor)
		{
			if (FileLockManager.TimeoutExceeded(fileName, accessor))
			{
				FileLockManager.UnlockByDeletingSyncConfiguration(fileName, accessor);
			}
			else
			{
				Log.Debug("Cannot execute operation because file '{0}' is being synced", fileName);

				throw new SynchronizationException(string.Format("File {0} is being synced", fileName));
			}
		}

		public void Rename(string name, string rename, Etag etag)
		{
			Storage.Batch(accessor =>
			{
				AssertFileIsNotBeingSynced(name, accessor);

				var existingFile = accessor.ReadFile(name);
				if (existingFile == null || existingFile.Metadata.Keys.Contains(SynchronizationConstants.RavenDeleteMarker))
					throw new FileNotFoundException();

				var renamingFile = accessor.ReadFile(rename);
				if (renamingFile != null && renamingFile.Metadata.ContainsKey(SynchronizationConstants.RavenDeleteMarker) == false)
					throw new InvalidOperationException("Cannot rename because file " + rename + " already exists");

				var metadata = existingFile.Metadata;

				if (etag != null && existingFile.Etag != etag)
					throw new ConcurrencyException("Operation attempted on file '" + name + "' using a non current etag")
					{
						ActualETag = existingFile.Etag,
						ExpectedETag = etag
					};
				
				Historian.UpdateLastModified(metadata);

				var operation = new RenameFileOperation
				{
					FileSystem = FileSystem.Name,
					Name = name,
					Rename = rename,
					MetadataAfterOperation = metadata
				};

				accessor.SetConfig(RavenFileNameHelper.RenameOperationConfigNameForFile(name), JsonExtensions.ToJObject(operation));
				accessor.PulseTransaction(); // commit rename operation config

				ExecuteRenameOperation(operation, etag);
			});

			Log.Debug("File '{0}' was renamed to '{1}'", name, rename);

			FileSystem.Synchronization.StartSynchronizeDestinationsInBackground();
		}

		public void ExecuteRenameOperation(RenameFileOperation operation, Etag etag)
		{
			var configName = RavenFileNameHelper.RenameOperationConfigNameForFile(operation.Name);
			Publisher.Publish(new FileChangeNotification
			{
				File = FilePathTools.Cannoicalise(operation.Name),
				Action = FileChangeAction.Renaming
			});

			Storage.Batch(accessor =>
			{
				var previousRenameTombstone = accessor.ReadFile(operation.Rename);

				if (previousRenameTombstone != null &&
					previousRenameTombstone.Metadata[SynchronizationConstants.RavenDeleteMarker] != null)
				{
					// if there is a tombstone delete it
					accessor.Delete(previousRenameTombstone.FullPath);
				}

				accessor.RenameFile(operation.Name, operation.Rename, true);
				accessor.UpdateFileMetadata(operation.Rename, operation.MetadataAfterOperation, null);

				// copy renaming file metadata and set special markers
				var tombstoneMetadata = new RavenJObject(operation.MetadataAfterOperation).WithRenameMarkers(operation.Rename);

				var putResult = accessor.PutFile(operation.Name, 0, tombstoneMetadata, true); // put rename tombstone

				accessor.DeleteConfig(configName);

				Search.Delete(operation.Name);
				Search.Index(operation.Rename, operation.MetadataAfterOperation, putResult.Etag);
			});

			Publisher.Publish(new ConfigurationChangeNotification { Name = configName, Action = ConfigurationChangeAction.Set });
			Publisher.Publish(new FileChangeNotification
			{
				File = FilePathTools.Cannoicalise(operation.Rename),
				Action = FileChangeAction.Renamed
			});
		}

		public void IndicateFileToDelete(string fileName, Etag etag)
		{
			var deletingFileName = RavenFileNameHelper.DeletingFileName(fileName);
			var fileExists = true;

			Storage.Batch(accessor =>
			{
				AssertDeleteOperationNotVetoed(fileName);

				var existingFile = accessor.ReadFile(fileName);

				if (existingFile == null)
				{
					// do nothing if file does not exist
					fileExists = false;
					return;
				}

				if (existingFile.Metadata[SynchronizationConstants.RavenDeleteMarker] != null)
				{
					// if it is a tombstone drop it
					accessor.Delete(fileName);
					fileExists = false;
					return;
				}

				if (etag != null && existingFile.Etag != etag)
					throw new ConcurrencyException("Operation attempted on file '" + fileName + "' using a non current etag")
					{
						ActualETag = existingFile.Etag,
						ExpectedETag = etag
					};

				var metadata = new RavenJObject(existingFile.Metadata).WithDeleteMarker();

				var renameSucceeded = false;

				int deleteVersion = 0;

				do
				{
					try
					{
						accessor.RenameFile(fileName, deletingFileName);
						renameSucceeded = true;
					}
					catch (FileExistsException) // it means that .deleting file was already existed
					{
						var deletingFileHeader = accessor.ReadFile(deletingFileName);

						if (deletingFileHeader != null && deletingFileHeader.Equals(existingFile))
						{
							fileExists = false; // the same file already marked as deleted no need to do it again
							return;
						}

						// we need to use different name to do a file rename
						deleteVersion++;
						deletingFileName = RavenFileNameHelper.DeletingFileName(fileName, deleteVersion);
					}
				} while (!renameSucceeded && deleteVersion < 128);

				if (renameSucceeded)
				{
					accessor.UpdateFileMetadata(deletingFileName, metadata, null);
					accessor.DecrementFileCount(deletingFileName);

					Log.Debug("File '{0}' was renamed to '{1}' and marked as deleted", fileName, deletingFileName);

					var configName = RavenFileNameHelper.DeleteOperationConfigNameForFile(deletingFileName);
					var operation = new DeleteFileOperation { OriginalFileName = fileName, CurrentFileName = deletingFileName };
					accessor.SetConfig(configName, JsonExtensions.ToJObject(operation));

					FileSystem.DeleteTriggers.Apply(trigger => trigger.AfterDelete(fileName));

					Publisher.Publish(new ConfigurationChangeNotification { Name = configName, Action = ConfigurationChangeAction.Set });
				}
				else
				{
					Log.Warn("Could not rename a file '{0}' when a delete operation was performed", fileName);
				}
			});

			if (fileExists)
			{
				Search.Delete(fileName);
				Search.Delete(deletingFileName);
			}
		}

		public Task CleanupDeletedFilesAsync()
		{
			var filesToDelete = new List<DeleteFileOperation>();

			Storage.Batch(accessor => filesToDelete = accessor.GetConfigsStartWithPrefix(RavenFileNameHelper.DeleteOperationConfigPrefix, 0, 10)
															  .Select(config => config.JsonDeserialization<DeleteFileOperation>())
															  .ToList());

			if (filesToDelete.Count == 0)
				return Task.FromResult<object>(null);

			var tasks = new List<Task>();

			foreach (var fileToDelete in filesToDelete)
			{
				var deletingFileName = fileToDelete.CurrentFileName;

				if (IsDeleteInProgress(deletingFileName))
					continue;

				if (IsUploadInProgress(fileToDelete.OriginalFileName))
					continue;

				if (IsSynchronizationInProgress(fileToDelete.OriginalFileName))
					continue;

				if (fileToDelete.OriginalFileName.EndsWith(RavenFileNameHelper.DownloadingFileSuffix)) // if it's .downloading file
				{
					if (IsSynchronizationInProgress(SynchronizedFileName(fileToDelete.OriginalFileName))) // and file is being synced
						continue;
				}

				Log.Debug("Starting to delete file '{0}' from storage", deletingFileName);

				var deleteTask = Task.Run(() =>
				{
					try
					{
						Storage.Batch(accessor => accessor.Delete(deletingFileName));
					}
					catch (Exception e)
					{
						Log.Warn(string.Format("Could not delete file '{0}' from storage", deletingFileName), e);
						return;
					}
					var configName = RavenFileNameHelper.DeleteOperationConfigNameForFile(deletingFileName);

					Storage.Batch(accessor => accessor.DeleteConfig(configName));

					Publisher.Publish(new ConfigurationChangeNotification
					{
						Name = configName,
						Action = ConfigurationChangeAction.Delete
					});

					Log.Debug("File '{0}' was deleted from storage", deletingFileName);
				});

				deleteFileTasks.AddOrUpdate(deletingFileName, deleteTask, (file, oldTask) => deleteTask);

				tasks.Add(deleteTask);
			}

			return Task.WhenAll(tasks);
		}

		public Task ResumeFileRenamingAsync()
		{
			var filesToRename = new List<RenameFileOperation>();

			Storage.Batch(accessor =>
			{
				var renameOpConfigs = accessor.GetConfigsStartWithPrefix(RavenFileNameHelper.RenameOperationConfigPrefix, 0, 10);

				filesToRename = renameOpConfigs.Select(config => config.JsonDeserialization<RenameFileOperation>()).ToList();
			});

			if (filesToRename.Count == 0)
				return Task.FromResult<object>(null);

			var tasks = new List<Task>();

			foreach (var item in filesToRename)
			{
				var renameOperation = item;

				if (IsRenameInProgress(renameOperation.Name))
					continue;

				Log.Debug("Starting to resume a rename operation of a file '{0}' to '{1}'", renameOperation.Name,
						  renameOperation.Rename);

				var renameTask = Task.Run(() =>
				{
					try
					{
						ExecuteRenameOperation(renameOperation, null);
						Log.Debug("File '{0}' was renamed to '{1}'", renameOperation.Name, renameOperation.Rename);

					}
					catch (Exception e)
					{
						Log.Warn(string.Format("Could not rename file '{0}' to '{1}'", renameOperation.Name, renameOperation.Rename), e);
						throw;
					}
				});

				renameFileTasks.AddOrUpdate(renameOperation.Name, renameTask, (file, oldTask) => renameTask);

				tasks.Add(renameTask);
			}

			return Task.WhenAll(tasks);
		}

		private static string SynchronizedFileName(string originalFileName)
		{
			return originalFileName.Substring(0, originalFileName.IndexOf(RavenFileNameHelper.DownloadingFileSuffix, StringComparison.InvariantCulture));
		}

		private bool IsSynchronizationInProgress(string originalFileName)
		{
			if (!FileLockManager.TimeoutExceeded(originalFileName, Storage))
				return true;
			return false;
		}

		private bool IsUploadInProgress(string originalFileName)
		{
			FileHeader deletedFile = null;
			Storage.Batch(accessor => deletedFile = accessor.ReadFile(originalFileName));

			if (deletedFile != null) // if there exists a file already marked as deleted
			{
				if (deletedFile.IsFileBeingUploadedOrUploadHasBeenBroken()) // and might be uploading at the moment
				{
					if (!uploadingFiles.ContainsKey(deletedFile.FullPath))
					{
						uploadingFiles.TryAdd(deletedFile.FullPath, deletedFile);
						return true; // first attempt to delete a file, prevent this time
					}
					var uploadingFile = uploadingFiles[deletedFile.FullPath];
					if (uploadingFile != null && uploadingFile.UploadedSize != deletedFile.UploadedSize)
					{
						return true; // if uploaded size changed it means that file is being uploading
					}
					FileHeader header;
					uploadingFiles.TryRemove(deletedFile.FullPath, out header);
				}
			}
			return false;
		}

		private bool IsDeleteInProgress(string deletingFileName)
		{
			Task existingTask;

			if (deleteFileTasks.TryGetValue(deletingFileName, out existingTask))
			{
				if (!existingTask.IsCompleted)
				{
					return true;
				}

				deleteFileTasks.TryRemove(deletingFileName, out existingTask);
			}
			return false;
		}

		private bool IsRenameInProgress(string fileName)
		{
			Task existingTask;

			if (renameFileTasks.TryGetValue(fileName, out existingTask))
			{
				if (!existingTask.IsCompleted)
				{
					return true;
				}

				renameFileTasks.TryRemove(fileName, out existingTask);
			}
			return false;
		}

		private class ReadFileToDatabase : IDisposable
		{
			private readonly byte[] buffer;
			private readonly BufferPool bufferPool;
			private readonly string filename;

			private readonly RavenJObject headers;

			private readonly Stream inputStream;
			private readonly ITransactionalStorage storage;

			private readonly OrderedPartCollection<AbstractFilePutTrigger> putTriggers;

			private readonly IHashEncryptor md5Hasher;
			public int TotalSizeRead;
			private int pos;

			public ReadFileToDatabase(BufferPool bufferPool, ITransactionalStorage storage, OrderedPartCollection<AbstractFilePutTrigger> putTriggers, Stream inputStream, string filename, RavenJObject headers)
			{
				this.bufferPool = bufferPool;
				this.inputStream = inputStream;
				this.storage = storage;
				this.putTriggers = putTriggers;
				this.filename = filename;
				this.headers = headers;
				buffer = bufferPool.TakeBuffer(StorageConstants.MaxPageSize);
				md5Hasher = Encryptor.Current.CreateHash();
			}

			public string FileHash { get; private set; }

			public void Dispose()
			{
				bufferPool.ReturnBuffer(buffer);
			}

			public async Task Execute()
			{
				while (true)
				{
					var read = await inputStream.ReadAsync(buffer);

					TotalSizeRead += read;

					if (read == 0) // nothing left to read
					{
						FileHash = IOExtensions.GetMD5Hex(md5Hasher.TransformFinalBlock());
						headers["Content-MD5"] = FileHash;
						storage.Batch(accessor =>
						{
							accessor.CompleteFileUpload(filename);
							putTriggers.Apply(trigger => trigger.AfterUpload(filename, headers));
						});
						return; // task is done
					}

					int retries = 50;
					bool shouldRetry;

					do
					{
						try
						{
							storage.Batch(accessor =>
							{
								var hashKey = accessor.InsertPage(buffer, read);
								accessor.AssociatePage(filename, hashKey, pos, read);
								putTriggers.Apply(trigger => trigger.OnUpload(filename, headers, hashKey, pos, read));
							});

							shouldRetry = false;
						}
						catch (ConcurrencyException)
						{
							if (retries-- > 0)
							{
								shouldRetry = true;
								Thread.Sleep(50);
								continue;
							}

							throw;
						}
					} while (shouldRetry);

					md5Hasher.TransformBlock(buffer, 0, read);

					pos++;
				}
			}
		}

		public class PutOperationOptions
		{
			public Guid? UploadId { get; set; }
			
			public bool PreserveTimestamps { get; set; }

			public DateTimeOffset? LastModified { get; set; }

			public long? ContentLength { get; set; }

			public long? ContentSize { get; set; }

			public bool TransferEncodingChunked { get; set; }
		}
	}
}