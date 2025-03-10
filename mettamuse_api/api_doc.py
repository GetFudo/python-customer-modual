from drf_yasg import openapi
from rest_framework.decorators import schema

class APIDOC:
    '''
        Utility for Swagger Documentations.
    '''

    GET_METTAMUSE_HOMEPAGE_MANUAL_PARAMS = [
        openapi.Parameter(name='Authorization', in_=openapi.IN_HEADER, type=openapi.TYPE_STRING, required=True),
        openapi.Parameter(name='currency', in_=openapi.IN_HEADER, type=openapi.TYPE_STRING, required=False),
        openapi.Parameter(name='homepageId', in_=openapi.IN_QUERY, type=openapi.TYPE_STRING,
        description="id for homepage to be retrive.", required=True, default="5dcd58c6bd5f672d74e152de")
    ]

    PATCH_METTAMUSE_HOMEPAGE_REQUEST_BODY = openapi.Schema(
        type=openapi.TYPE_OBJECT,
        required=['data'],
        properties={
        'data': openapi.Schema(type=openapi.TYPE_OBJECT, description="data",
            properties={
                'seo': openapi.Schema(type=openapi.TYPE_OBJECT, description='sectionName',
                    properties={
                        'title': openapi.Schema(type=openapi.TYPE_STRING, description="data to update",
                            example="Online Shopping Site for Lifestyle & More. Best Offers!"),
                        'metaTags': openapi.Schema(type=openapi.TYPE_STRING, description="data to update",
                            example="MettaMuse,Shopping,Clothes,Online Shopping",
                        )}
                    ),
            }),
        'homepageId': openapi.Schema(type=openapi.TYPE_STRING, description='storeId', example="60c0c1b91faefe093a456b59")
    })

    PATCH_METTAMUSE_HOMEPAGE_MANUAL_PARAMS = [
        openapi.Parameter(name='Authorization', in_=openapi.IN_HEADER, type=openapi.TYPE_STRING, required=True),
    ]


APIDocObj = APIDOC()
