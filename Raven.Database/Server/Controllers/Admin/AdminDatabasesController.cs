﻿using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Web.Http;
using Mono.CSharp;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Extensions;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers.Admin
{
	[RoutePrefix("")]
	public class AdminDatabasesController : BaseAdminController
	{
		[HttpGet][Route("admin/databases/{*id}")]
		public HttpResponseMessage DatabasesGet(string id)
		{
			if (IsSystemDatabase(id))
			{
				//fetch fake (empty) system database document
				var systemDatabaseDocument = new DatabaseDocument { Id = Constants.SystemDatabase };
				return GetMessageWithObject(systemDatabaseDocument);
			}

			var docKey = "Raven/Databases/" + id;

			var document = Database.Documents.Get(docKey, null);
			if (document == null)
				return GetMessageWithString("Database " + id + " not found", HttpStatusCode.NotFound);

			var dbDoc = document.DataAsJson.JsonDeserialization<DatabaseDocument>();
			dbDoc.Id = id;
			DatabasesLandlord.Unprotect(dbDoc);
			return GetMessageWithObject(dbDoc);
		}

		[HttpPut]
		[Route("admin/databases/{*id}")]
		public async Task<HttpResponseMessage> DatabasesPut(string id)
		{
			if (id == null)
				return GetMessageWithString(string.Format("An empty name is forbidden for use!"), HttpStatusCode.BadRequest);
			if (id.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0)
				return GetMessageWithString(string.Format("The name '{0}' contains charaters that are forbidden for use!", id), HttpStatusCode.BadRequest);
			if (Array.IndexOf(Constants.WindowsReservedFileNames, id.ToLower()) >= 0)
				return GetMessageWithString(string.Format("The name '{0}' is forbidden for use!", id), HttpStatusCode.BadRequest);
			if ((Environment.OSVersion.Platform == PlatformID.Unix) && (id.Length > Constants.LinuxMaxFileNameLength) && (Database.Configuration.DataDirectory.Length + id.Length > Constants.LinuxMaxPath))
			{
				int theoreticalMaxFileNameLength = Constants.LinuxMaxPath - Database.Configuration.DataDirectory.Length;
				int maxfileNameLength = (theoreticalMaxFileNameLength > Constants.LinuxMaxFileNameLength) ? Constants.LinuxMaxFileNameLength : theoreticalMaxFileNameLength;
				return GetMessageWithString(string.Format("Invalid name for a database! Databse name cannot exceed {0} characters", maxfileNameLength), HttpStatusCode.BadRequest);
			}
			else{ //windows platform
				if (Path.Combine(Database.Configuration.DataDirectory, id).Length > Constants.WindowsMaxPath)
				{
					int maxfileNameLength = Constants.WindowsMaxPath - Database.Configuration.DataDirectory.Length;
					return GetMessageWithString(string.Format("Invalid name for a database! Databse name cannot exceed {0} characters", maxfileNameLength), HttpStatusCode.BadRequest);
				}
			}

			if (IsSystemDatabase(id))
			{
				return GetMessageWithString("System Database document cannot be changed", HttpStatusCode.Forbidden);
			}
			var docKey = "Raven/Databases/" + id;
			var existingDatabase = Database.Documents.Get(docKey, null);
			if (existingDatabase != null)
			{
				return GetMessageWithString(string.Format("Database with the name '{0}' already exists", id), HttpStatusCode.BadRequest);
			}

			var dbDoc = await ReadJsonObjectAsync<DatabaseDocument>();
			if (dbDoc.Settings.ContainsKey("Bundles") && dbDoc.Settings["Bundles"].Contains("Encryption"))
			{
				if (!dbDoc.SecuredSettings.ContainsKey(Constants.EncryptionKeySetting) ||
				    !dbDoc.SecuredSettings.ContainsKey(Constants.AlgorithmTypeSetting))
				{
					return GetMessageWithString(string.Format("Failed to create '{0}' database, becuase of not valid encryption configuration.", id), HttpStatusCode.BadRequest);
				}
			}

			DatabasesLandlord.Protect(dbDoc);
			var json = RavenJObject.FromObject(dbDoc);
			json.Remove("Id");

			Database.Documents.Put(docKey, null, json, new RavenJObject(), null);

			return GetEmptyMessage();
		}

		[HttpDelete][Route("admin/databases/{*id}")]
		public HttpResponseMessage DatabasesDelete(string id)
		{
			if (IsSystemDatabase(id))
				return GetMessageWithString("System Database document cannot be deleted", HttpStatusCode.Forbidden);

			var docKey = "Raven/Databases/" + id;
			var configuration = DatabasesLandlord.CreateTenantConfiguration(id);
			var databasedocument = Database.Documents.Get(docKey, null);

			if (configuration == null)
				return GetEmptyMessage();

			Database.Documents.Delete(docKey, null, null);
			bool result;

			if (bool.TryParse(InnerRequest.RequestUri.ParseQueryString()["hard-delete"], out result) && result)
			{
				IOExtensions.DeleteDirectory(configuration.DataDirectory);
				IOExtensions.DeleteDirectory(configuration.IndexStoragePath);

				if (databasedocument != null)
				{
					var dbDoc = databasedocument.DataAsJson.JsonDeserialization<DatabaseDocument>();
					if (dbDoc != null && dbDoc.Settings.ContainsKey(Constants.RavenLogsPath))
						IOExtensions.DeleteDirectory(dbDoc.Settings[Constants.RavenLogsPath]);
				}
			}

			return GetEmptyMessage();
		}

	}
}