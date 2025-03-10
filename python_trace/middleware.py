import time
from python_trace import Counter, Histogram, start_http_server

REQUEST_LATENCY = Histogram('http_request_duration_ms', 'Duration of HTTP requests in ms',
                            ['app_name', 'method', 'route', 'code'],
                            buckets= [0.10, 5, 15, 50, 100, 200, 300, 400, 500, 600, 700, 800, 1000, 1200, 1400, 1600,
                                      1800, 2000, 2400, 2800, 3200, 3600, 4000, 5000, 6000, 7000, 8000, 9000, 10000,
                                      12000, 14000, 16000, 18000, 20000, 22000, 24000, 25000, 26000, 30000, 34000, 35000
                                      ]
                            )

class MonitoringMiddleware:
    
    
    def __init__(self, get_response):
        self.get_response = get_response
        start_http_server(8004)
    # One-time configuration and initialization.
    
    def __call__(self, request):
        
        request.start_time = (time.time())*1000
        
        
        # Code to be executed for each request before
        # the view (and later middleware) are called.
        
        response = self.get_response(request)
        

        
        REQUEST_LATENCY.labels('club_app', request.META['REQUEST_METHOD'], request.META['PATH_INFO'], response.status_code).observe(int((time.time())*1000 - request.start_time))
        
        
        # Code to be executed for each request/response after
        # the view is called.
        return response
