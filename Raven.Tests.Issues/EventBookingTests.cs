using System.IO;
using Raven.Client.Embedded;
using Raven.Imports.Newtonsoft.Json;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
	public class EventBookingTests : RavenTest
	{
		[Fact]
		public void Read_only_property_is_accessed_during_deserialization()
		{
			var serializer = new JsonSerializer { ConstructorHandling = ConstructorHandling.AllowNonPublicDefaultConstructor };

			var stringWriter = new StringWriter();
			serializer.Serialize( new JsonTextWriter( stringWriter ), new DomainClass { IntVal = 1 } );

			var json = stringWriter.ToString();

			serializer.Deserialize<DomainClass>( new JsonTextReader( new StringReader( json ) ) );
		}
	}

	public class RavenSerializationFailureTests
	{
		private EmbeddableDocumentStore _store;

		[Fact]
		public void Serialization_Of_VenueEvent_Passes()
		{
			Setup();

			DomainClass sample;

			using( var session = _store.OpenSession() )
			{
				sample = new DomainClass();

				session.Store( sample );
				session.SaveChanges();
			}

			using( var session = _store.OpenSession() )
			{
				session.Load<DomainClass>( sample.Id );
			}

			Teardown();
		}

		public void Setup()
		{
			_store = new EmbeddableDocumentStore
			{
				RunInMemory = true
			};

			_store.Initialize();
		}

		public void Teardown()
		{
			_store.Dispose();
		}
	}

	public class DomainClass
	{
		public string Id { get; protected set; }

		// We have a read-only property that relies on a writable property.
		// This property should be serialized; but should not be referenced 
		// during the deserialization process. Because the object is in an 
		// invalid state, 
		public ValueClass ReadOnlyVal
		{
			get { return 100 / IntVal > 0 ? new ValueClass() : null; }
		}

		public int IntVal { get; set; }
	}

	public class ValueClass
	{
	}
}