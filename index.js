// const { ApolloServer, gql } = require('apollo-server-cloud-functions');
const { ApolloServer, gql } = require('apollo-server');
const { BigQuery } = require('@google-cloud/bigquery');

const traffics = [
  {date: "20210901", traffic: 553.0, dwells: 123.0, total_dwell_time: 2868.0}
];

const bigqueryClient = new BigQuery();

// Construct a schema, using GraphQL schema language
const typeDefs = gql`
  type Traffic {
    date: String
    traffic: Float
    dwells: Float
    total_dwell_time: Float
  }

  type Query {
    traffics(dates: [String]!, storeIds: [Int]!, productIds: [Int]!): [Traffic]
  }
`;

// Resolvers define the technique for fetching the types defined in the
// schema. This resolver retrieves books from the "books" array above.
const resolvers = {
  Query: {
    async traffics(parent, args, context, info) {
      console.log(args)

      const storeIds = args['storeIds'] ? args['storeIds'].join(', ') : ''
      const productIds = args['productIds'] ? args['productIds'].join(', ') : ''
      const dates = args['dates'] ? args['dates'].map(date => `'${date}'`).join(', ') : ''

      const query = `select date, sum(traffic) traffic, sum(dwells) dwells, sum(total_dwell_time) total_dwell_time
        FROM (
          SELECT 
          date, product_id, (traffic/ARRAY_LENGTH(product_ids)) traffic, (dwells/ARRAY_LENGTH(product_ids)) dwells, (total_dwell_time/ARRAY_LENGTH(product_ids)) total_dwell_time
          from retail_next.placements_daily
          join UNNEST(product_ids) AS product_id
          WHERE EXISTS (SELECT *
                        FROM UNNEST(product_ids) AS x
                        WHERE x in (${productIds}))
          AND date IN (${dates})
          AND store_id IN (${storeIds})
        ) t1
        WHERE t1.product_id IN (${productIds})
        group by 1`;
      console.log(query);

      const [job] = await bigqueryClient.createQueryJob({ query: query });
      console.log(`Job ${job.id} started.`);
      const [rows] = await job.getQueryResults();
      console.log('Rows:', rows);

      return rows
    },
  },
};

const server = new ApolloServer({
  typeDefs,
  resolvers,
  csrfPrevention: true,
  cache: 'bounded',
});

// The `listen` method launches a web server.
server.listen().then(({ url }) => {
  console.log(`🚀  Server ready at ${url}`);
});

// exports.handler = server.createHandler();
