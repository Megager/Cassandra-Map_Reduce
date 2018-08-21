using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cassandra.Cassandra.Entity;
using Cassandra.Data.Linq;
using Cassandra.Mapping;

namespace Cassandra.Cassandra
{
    public class Repository<TEntity> 
        where TEntity : IEntity
    {
        private readonly ISession _session;

        public Repository()
        {
            Cluster cluster = Cluster.Builder().AddContactPoint("127.0.0.1").Build();
            _session = cluster.Connect("cycling");
        }

        public RowSet ExecuteQuery(string query)
        {
            return _session.Execute(query);
        }

        public IEnumerable<TEntity> GetAll()
        {
            return new Table<TEntity>(_session, MappingConfiguration.Global, "orders").Execute();
        }
    }
}
