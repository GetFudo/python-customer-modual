module.exports = {
  apps : [
      {
            name: 'search',
            script: 'manage.py',
            args: 'runserver 0.0.0.0:8002',
            instances: 1,
            autorestart: true,
            watch: false,
            max_memory_restart: '1G',
      },
      {
            name: 'inventory-grpc',
            script: 'inventory_validation_app/validate_inventory_grpc.py',
            instances: 1,
            autorestart: true,
            watch: false,
            max_memory_restart: '1G',
      },
//      {
//            name: 'mongoStream',
//            script: 'search/stream.py',
//            instances: 1,
//            autorestart: true,
//            watch: false,
//            max_memory_restart: '4G',
//      }
      // {
      //       name: 'PopularStoreStream',
      //       script: 'search/storeCronJob.py',
      //       instances: 1,
      //       autorestart: true,
      //       watch: false,
      //       max_memory_restart: '4G',
      // }
  ]
};
