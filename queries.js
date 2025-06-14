const { MongoClient } = require('mongodb');

const uri = 'mongodb://localhost:27017'

function performQueriesTask3(collection) {
    const page = 1;
    const pageSize = 5;  

    result = collection.find([
        {
            inStock: true,
            publishedYear: { $gt: 2010 }
        },
        {
            title: 1,
            author: 1,
            price: 1,
            _id: 0
        }
    ]).sort({ price: 1 })
     .skip((page - 1) * pageSize)
     .limit(pageSize);

    return result;
};


function performQueriesTask4(collection) {
    averagePricePerGenre = collection.aggregate([
      {
          $group: {
              _id: "$genre", 
              averagePrice: { $avg: "$price" }
          }
      },
      {
          $project: {
              _id: 0,
              genre: "$_id", 
              averagePrice: 1 
          }
      }
  ]);

    authorWithMostBooks = collection.aggregate([
        {
            $group: {
                _id: "$author", 
                bookCount: { $sum: 1 } 
            }
        },
        {
            $sort: {
                bookCount: -1 
            }
        },
        {
            $limit: 1 
        },
        {
            $project: {
                _id: 0,
                author: "$_id",
                bookCount: 1
            }
        }
    ]);

    queryByDecade = collection.aggregate([
        {
          $group: {
            _id: {
              $floor: { $divide: ["$publishedYear", 10]}
            },
            count: { $sum: 1}
          }
        },
        {
            $project: {
                decade: { $multiply: ["$_id", 10] }, 
                count: 1, 
                _id: 0
            }
        },
        {
            $sort: {
                decade: 1
            }
        }
    ]);

  return [averagePricePerGenre, authorWithMostBooks, queryByDecade]
  
};


function performQueriesTask5(collection) {
    collection.createIndex({title: 1});
    collection.createIndex({author: 1, publishedYear: 1});
     return collection.find({ title: "Some Book Title" }).explain("executionStats");
};


module.exports = {
    performQueriesTask3,
    performQueriesTask4,
    performQueriesTask5
};
