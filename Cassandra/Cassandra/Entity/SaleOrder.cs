using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cassandra.Cassandra.Entity
{
    public class SaleOrder : IEntity
    {
        public string Region { get; set; }
        public string Country { get; set; }
        public string Item_Type { get; set; }
        public string Sales_Channel { get; set; }
        public string Order_Priority { get; set; }
        public string Order_Date { get; set; }
        public string Order_ID { get; set; }
        public string Ship_Date { get; set; }
        public double Units_Sold { get; set; }
        public double Unit_Price { get; set; }
        public double Unit_Cost { get; set; }
        public double Total_Revenue { get; set; }
        public double Total_Cost { get; set; }
        public double Total_Profit { get; set; }
    }
}
