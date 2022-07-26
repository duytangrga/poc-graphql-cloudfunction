// const { ApolloServer, gql } = require('apollo-server-cloud-functions');
const { ApolloServer, gql } = require('apollo-server');
const { BigQuery } = require('@google-cloud/bigquery');
const moment = require('moment');

const bigqueryClient = new BigQuery();

// Construct a schema, using GraphQL schema language
const typeDefs = gql`
  type Traffic {
    date: String
    traffic: Float
    dwells: Float
    total_dwell_time: Float
  }

  type Demo {
    date: String
    demo_count: Float
  }

  type Query {
    traffics(dates: [String]!, storeIds: [Int]!, productIds: [Int]!): [Traffic]
    demos(dates: [String]!, storeIds: [Int]!, productIds: [Int]!): [Demo]
  }
`;

// Resolvers define the technique for fetching the types defined in the
// schema. This resolver retrieves books from the "books" array above.
const resolvers = {
  Query: {
    async traffics(parent, args, context, info) {
      const storeIds = args['storeIds'] ? args['storeIds'].join(', ') : ''
      const productIds = args['productIds'] ? args['productIds'].join(', ') : ''
      const dates = args['dates'] ? args['dates'].map(date => `'${moment(date).format('YYYYMMDD')}'`).join(', ') : ''

      if (!storeIds || !productIds || !dates) {
        return []
      }

      const query = `SELECT date, sum(traffic) traffic, sum(dwells) dwells, sum(total_dwell_time) total_dwell_time
        FROM (
          SELECT 
          date, product_id, (traffic/ARRAY_LENGTH(product_ids)) traffic, (dwells/ARRAY_LENGTH(product_ids)) dwells, (total_dwell_time/ARRAY_LENGTH(product_ids)) total_dwell_time
          FROM retail_next.placements_daily
          join UNNEST(product_ids) AS product_id
          WHERE EXISTS (SELECT *
                        FROM UNNEST(product_ids) AS x
                        WHERE x IN (${productIds}))
          AND date IN (${dates})
          AND store_id IN (${storeIds})
        ) t1
        WHERE t1.product_id IN (${productIds})
        GROUP BY 1`;

      const [job] = await bigqueryClient.createQueryJob({ query: query });
      const [rows] = await job.getQueryResults();

      return rows
    },
    async demos(parent, args, context, info) {
      const storeIds = args['storeIds'] ? args['storeIds'].join(', ') : ''
      const productIds = args['productIds'] ? args['productIds'].join(', ') : ''
      const dates = args['dates'] ? args['dates'].map(date => `'${moment(date).format('YYYY-MM-DD')}'`) : []

      if (!storeIds || !productIds || !dates || dates.length < 1) {
        return []
      }

      const query = `SELECT FORMAT_DATE('%Y%m%d', DATE(TIMESTAMP(timestamp), 'America/Los_Angeles')) AS date, SUM(value) AS demo_count
        FROM metrics.displays
        WHERE
          product_id IN (${productIds})
          AND store_id IN (${storeIds})
          AND DATE(TIMESTAMP(timestamp), 'America/Los_Angeles') >= ${dates[0]}
          AND DATE(TIMESTAMP(timestamp), 'America/Los_Angeles') <= ${dates[dates.length - 1]}
          AND metric_name = 'demo.completed'
        GROUP BY 1
      `;

      console.log(query)

      const [job] = await bigqueryClient.createQueryJob({ query: query });
      const [rows] = await job.getQueryResults();

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
