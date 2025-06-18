const { MongoClient } = require('mongodb');

const uri = 'mongodb://localhost:27017';
const dbName = 'plp_bookstore';
const collectionName = 'books';

let client; 
let collection; 

// Function to get a connected MongoClient instance
const getClient = async (mongoUri) => {
    const mongoClient = new MongoClient(mongoUri);
    await mongoClient.connect();
    console.log('Connected to MongoDB server');
    return mongoClient;
};

// --- Task Functions ---

function performQueriesTask3(collection) {
    const page = 1;
    const pageSize = 5;

    const cursor = collection.find({
        inStock: true,
        publishedYear: { $gt: 2010 }
    }).project({ 
        title: 1,
        author: 1,
        price: 1,
        _id: 0
    })
    .sort({ price: 1 })
    .skip((page - 1) * pageSize)
    .limit(pageSize);

    return cursor; 
}

function performQueriesTask4(collection) {
    const averagePricePerGenreCursor = collection.aggregate([
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
        },
        {
            $sort: { genre: 1 } 
        }
    ]);

    const authorWithMostBooksCursor = collection.aggregate([
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

    const queryByDecadeCursor = collection.aggregate([
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
                _id: 0, 
                decade: { $multiply: ["$_id", 10] }, 
                count: 1
            }
        },
        {
            $sort: {
                decade: 1
            }
        }
    ]);

    return { averagePricePerGenreCursor, authorWithMostBooksCursor, queryByDecadeCursor };
}

async function performQueriesTask5(collection) { 
    try {
        // Await the index creation to ensure they complete
        await collection.createIndex({title: 1});
        console.log("Index on 'title' created successfully.");

    } catch (err) {
        if (err.code === 85 || err.message.includes('Index already exists')) {
            console.warn("Index on 'title' already exists, skipping creation.");
        } else {
            console.error("Error creating title index:", err);
            throw err; 
        }
    }

    try {
        await collection.createIndex({author: 1, publishedYear: 1});
        console.log("Compound index on 'author, publishedYear' created successfully.");
    } catch (err) {
        if (err.code === 85 || err.message.includes('Index already exists')) {
            console.warn("Compound index on 'author, publishedYear' already exists, skipping creation.");
        } else {
            console.error("Error creating author/publishedYear index:", err);
            throw err; 
        }
    }

    return collection.find({ title: "Some Book Title" }).explain("executionStats");
}


(async () => {
    try {
        client = await getClient(uri);
        const db = client.db(dbName);
        collection = db.collection(collectionName);

        // --- Execute Tasks ---

        // Task 3
        console.log("--- Task 3 Results ---");
        const task3Cursor = performQueriesTask3(collection);
        const task3Results = await task3Cursor.toArray(); // Await to get the actual documents
        console.log('Paginated & Projected Books:', task3Results);

        // Task 4
        console.log("\n--- Task 4 Results ---");
        const { averagePricePerGenreCursor, authorWithMostBooksCursor, queryByDecadeCursor } = performQueriesTask4(collection);

        console.log('Average Price Per Genre:');
        const avgPriceResults = await averagePricePerGenreCursor.toArray();
        console.log(avgPriceResults);

        console.log('\nAuthor With Most Books:');
        const authorMostBooksResults = await authorWithMostBooksCursor.toArray();
        console.log(authorMostBooksResults);

        console.log('\nBooks by Decade:');
        const queryDecadeResults = await queryByDecadeCursor.toArray();
        console.log(queryDecadeResults);


        // Task 5 - Await this call now as it's an async function
        console.log("\n--- Task 5 Results ---");
        const explanation = await performQueriesTask5(collection);
        console.log('Explanation for "Some Book Title" query:', JSON.stringify(explanation, null, 2));

    } catch (error) {
        console.error('An error occurred in the main execution block:', error);
    } finally {
        if (client) {
            await client.close();
            console.log('Disconnected from MongoDB server');
        }
    }
})();
