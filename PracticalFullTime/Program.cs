using System;
using System.Collections.Generic;
using System.Linq;

namespace RetakeExam
{
    public class Product
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public double Price { get; set; }

        public Product(int id, string name, string description, double price)
        {
            Id = id;
            Name = name;
            Description = description;
            Price = price;
        }

    }

    public class Customer
    {
        public int Id { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public int Age { get; set; }

        public Customer(int id, string firstName, string lastName, int age)
        {
            Id = id;
            FirstName = firstName;
            LastName = lastName;
            Age = age;
        }
    }

    public class Order
    {
        public int CustomerId { get; set; }
        public int ProductId { get; set; }
        public DateTime OrderDate { get; set; }
        public int Amount { get; set; }

        public Order(int customerId, int productId, DateTime orderDate, int amount)
        {
            CustomerId = customerId;
            ProductId = productId;
            OrderDate = orderDate;
            Amount = amount;
        }
    }

    public static class MapReduce
    {
        public static IEnumerable<T2> Map<T1, T2>(this IEnumerable<T1> collection, Func<T1, T2> transformation)
        {
            T2[] result = new T2[collection.Count()];
            for (int i = 0; i < collection.Count(); i++)
            {
                result[i] = transformation(collection.ElementAt(i));
            }
            return result;
        }

        public static T2 Reduce<T1, T2>(this IEnumerable<T1> collection, T2 init, Func<T2, T1, T2> operation)
        {
            T2 result = init;
            // TODO 1: complete the implementation of the reduce function (5 pts)
            //.....

            return result;
        }

        public static IEnumerable<Tuple<T1, T2>> Join<T1, T2>(this IEnumerable<T1> table1, IEnumerable<T2> table2, Func<T1, T2, bool> condition)
        {
            return
              Reduce(table1, new List<Tuple<T1, T2>>(),
                (queryResult, x) =>
                {
                    List<Tuple<T1, T2>> combination =
                    //TODO 2a: complete the implementation of the join function (2.5 pts)
                            //......  
                                           =>
                        {
                          Tuple<T1, T2> row = new Tuple<T1, T2>(x, y);
                          //TODO 2b: complete the implementation of the join function (2.5 pts)
                          //.......
                          return c;
                });
                    queryResult.AddRange(combination);
                    return queryResult;
                });
        }
    }

    class Program
    {
        public static Product[] ProductTable =
        {
      new Product(1, "PS4", "Game console from Sony", 350.99),
      new Product(2, "Xbox", "Game console from Microsoft", 299.99 ),
      new Product(3, "USB2 Mouse", "logitec gamer mouse",10),
      new Product(4, "Call Of Duty PS4", "PS4 console game 16+",10.0),
      new Product(5, "Call Of Duty Xbox", "Xbox console game 16+",12.0),
      new Product(6, "60 SSD", "Solid state disk 60GB", 40.0)
    };

        public static Customer[] CustomerTable =
        {
      new Customer(0, "John", "Doe", 35),
      new Customer(1, "Jane", "Doe", 28),
      new Customer(2, "Max", "Payne", 45),
      new Customer(3, "Lara", "Croft", 25),
      new Customer(4, "Rachel", "Jackson", 28),
      new Customer(5, "Michael", "Knight", 40)
    };

        public static Order[] OrderTable =
        {
      new Order(1,1,new DateTime(2000, 3, 15),2),
      new Order(1,2,new DateTime(2017, 5, 1),1),
      new Order(2,5,new DateTime(2016, 11, 10),4),
      new Order(2,3,new DateTime(2019, 1, 23),1),
      new Order(2,5,new DateTime(2019, 12, 14),2),
      new Order(3,1,new DateTime(2018, 5, 14),1),
      new Order(3,4,new DateTime(2017, 8, 22),3),
      new Order(5,2,new DateTime(2019, 5, 15),5),
      new Order(5,6,new DateTime(2018, 8, 2),1),
      new Order(5,3,new DateTime(2019, 6, 7),6)
    };

        static void Main(string[] args)
        {
            /* 
             * SELECT p.Id, p.Name FROM Product p
             * WHERE p.price >= 250.0
             */

            var q1 =
              ProductTable.Reduce(
              new List<Product>(),
              (filteredProducts, product) =>
              {
                  //TODO 3: complete the implementation of query 1 (5 pts)
                  //....
              }
              ).Map(r => new { r.Id, r.Name });


            /*
             * SELECT p.Name, p.Price, o.OrderDate FROM Products p 
             * INNER JOIN Order o ON p.Id = o.ProductId 
             * WHERE o.OrderDate < '2005-02-01'
             */

            var q2 =
            ProductTable.Join(
              //TODO 4: complete the implementation of query 2 (5 pts)
              OrderTable,
              (p, o) =>  //....
              )
            .Map(r => new { r.Item1.Name, r.Item1.Price, r.Item2.OrderDate });


            /*
             * SELECT c.FirstName, c.LastName, p.Name, o.Amount FROM Customer c 
             * INNER JOIN Order o ON c.Id = o.CustomerId 
             * INNER JOIN Product p ON p.Id = o.ProductId 
             * WHERE o.Amount > 4
             */

            var q3 =
              CustomerTable.Join(
                //TODO 5a: complete the implementation of query 3 (3 pts)
                //......
                )
              .Join(
                //TODO 5b: complete the implementation of query 3 (3 pts)
                //......
                )
              //TODO 5c: complete the implementation of query 3 (4 pts)
              .Map(r => new //.....
              );
        }
    }
}
