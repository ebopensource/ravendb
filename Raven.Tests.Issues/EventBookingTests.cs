using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Reflection;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Serialization;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
	public class EventBookingTests : RavenTest
	{
		// 3517
		[Fact]
		public void Read_only_property_is_accessed_during_deserialization()
		{
			var serializer = new JsonSerializer {ConstructorHandling = ConstructorHandling.AllowNonPublicDefaultConstructor};

			var stringWriter = new StringWriter();
			serializer.Serialize(new JsonTextWriter(stringWriter), new DomainClass {IntVal = 1});

			var json = stringWriter.ToString();

			serializer.Deserialize<DomainClass>(new JsonTextReader(new StringReader(json)));
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

			using (var session = _store.OpenSession())
			{
				sample = new DomainClass();

				session.Store(sample);
				session.SaveChanges();
			}

			using (var session = _store.OpenSession())
			{
				session.Load<DomainClass>(sample.Id);
			}

			Teardown();
		}

		public void Setup()
		{
			_store = new EmbeddableDocumentStore
			{
				RunInMemory = true,
				Conventions = Convention
			};

			_store.Initialize();
		}

		public void Teardown()
		{
			_store.Dispose();
		}

		public static DocumentConvention Convention
		{
			get
			{
				var convention = new DocumentConvention
				{
					FindTypeTagName = t => Inflector.Singularize( t.Name ),
					CustomizeJsonSerializer = serializer =>
					{
						serializer.Converters.Add( new TypeSafeEnumConverter() );
						serializer.ContractResolver = new CustomContractResolver();
					},
					IdentityPartsSeparator = "-",
					DisableProfiling = true
				};

				return convention;
			}
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
			get { return 100/IntVal > 0 ? new ValueClass() : null; }
		}

		public int IntVal { get; set; }
	}

	public class ValueClass
	{
	}

	public class CustomContractResolver : DefaultRavenContractResolver
	{
		public CustomContractResolver()
			: base( true )
		{
			DefaultMembersSearchFlags = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;
		}

		protected override JsonContract CreateContract( Type objectType )
		{
			var contract = base.CreateContract( objectType );
			if( typeof( TypeSafeEnum ).IsAssignableFrom( objectType ) )
			{
				contract.Converter = new TypeSafeEnumConverter();
			}
			return contract;
		}
	}

	public class TypeSafeEnumConverter : JsonConverter
	{
		public override void WriteJson( JsonWriter writer, object value, JsonSerializer serializer )
		{
			var item = (TypeSafeEnum) value;
			writer.WriteValue( item.Value );
			writer.Flush();
		}

		public override object ReadJson( JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer )
		{
			var value = reader.Value;
			if( value == null )
				return null;

			var intVal = Int32.Parse( value.ToString() );
			var enumType = typeof( TypeSafeEnum );

			// warning: constant string method name
			const string methodName = "FromValue";

			var methodInfo = enumType.GetMethod( methodName, new[] { typeof( int ) } );
			var method = methodInfo.MakeGenericMethod( objectType );

			var item = method.Invoke( null, new object[] { intVal } );
			return item;
		}

		public override bool CanConvert( Type objectType )
		{
			return objectType == typeof( TypeSafeEnum );
		}
	}

	public abstract class TypeSafeEnum
	{
		protected TypeSafeEnum( int value, Func<string> displayName )
		{
			Value = value;
			_displayName = displayName;
		}

		public int Value { get; private set; }

		private readonly Func<string> _displayName;

		public string DisplayName
		{
			get { return _displayName(); }
		}

		public static implicit operator int( TypeSafeEnum e )
		{
			return e.Value;
		}

		public override bool Equals( object obj )
		{
			var otherValue = obj as TypeSafeEnum;

			if( otherValue == null )
			{
				return false;
			}

			var typeMatches = GetType() == obj.GetType();
			var valueMatches = Value.Equals( otherValue.Value );

			return typeMatches && valueMatches;
		}

		public static bool operator ==( TypeSafeEnum a, TypeSafeEnum b )
		{
			if( ReferenceEquals( a, null ) && ReferenceEquals( b, null ) )
				return true;
			if( ReferenceEquals( a, null ) || ReferenceEquals( b, null ) )
				return false;
			return a.Equals( b );
		}

		public static bool operator !=( TypeSafeEnum a, TypeSafeEnum b )
		{
			return !( a == b );
		}

		public static IEnumerable<T> GetAll<T>() where T : TypeSafeEnum
		{
			var type = typeof( T );
			var fields = type.GetFields( BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly );
			var items = ( from field in fields
						  let instance = (T) Activator.CreateInstance( type, true )
						  select field.GetValue( instance ) ).OfType<T>();
			return items.ToList();
		}

		public static T FromValue<T>( int value ) where T : TypeSafeEnum
		{
			var matchingItem = Parse<T, int>( value, "value", item => item.Value == value );
			return matchingItem;
		}

		[ExcludeFromCodeCoverage]
		public override int GetHashCode()
		{
			return Value.GetHashCode();
		}

		[ExcludeFromCodeCoverage]
		public override string ToString()
		{
			return DisplayName;
		}

		private static T Parse<T, TK>( TK value, string description, Func<T, bool> predicate )
			where T : TypeSafeEnum
		{
			var matchingItem = GetAll<T>().FirstOrDefault( predicate );

			if( matchingItem == null )
			{
				var message = string.Format( "'{0}' is not a valid {1} in {2}", value, description, typeof( T ) );
				throw new ApplicationException( message );
			}

			return matchingItem;
		}

		protected TypeSafeEnum()
		{
		}
	}
}