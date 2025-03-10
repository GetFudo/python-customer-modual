module.exports = {
  apps : [{
    name: 'category-view',
    script: 'categoryviewconsume.py',
    instances: 1,
    autorestart: true,
    watch: false,
    max_memory_restart: '1G',
    interpreter:'/opt/python_api/search/enve/bin/python3'
  }]
};
