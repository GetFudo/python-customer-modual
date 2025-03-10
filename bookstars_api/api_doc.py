from drf_yasg import openapi

class APIDOC:
    '''
        Utility for Swagger Documentations.
    '''

    GET_ARTNICHE_MANUAL_PARAMS = [
        openapi.Parameter(
            name='Authorization', in_=openapi.IN_HEADER, type=openapi.TYPE_STRING, required=True, description="authorization token"),
        openapi.Parameter(
            name='language', default="en", in_=openapi.IN_HEADER, type=openapi.TYPE_STRING, required=True, description="language, for language support"),
        openapi.Parameter(
            name='search',
            required=False,
            default="",
            in_=openapi.IN_QUERY,
            type=openapi.TYPE_STRING,
            description="search perticular art/niche here."
        ),
        openapi.Parameter(
            name='from',
            default="0",
            in_=openapi.IN_QUERY,
            type=openapi.TYPE_STRING,
            description="starting data out of penCount."
        ),
        openapi.Parameter(
            name='to',
            default="20",
            in_=openapi.IN_QUERY,
            type=openapi.TYPE_STRING,
            description="ending data out of penCount."
        )
    ]

    GET_NICHE_RELATED_ARTISTS_MANUAL_PARAMS = [
        openapi.Parameter(
            name='Authorization', in_=openapi.IN_HEADER, type=openapi.TYPE_STRING, required=True, description="authorization token"),
        openapi.Parameter(
            name='language', default="en", in_=openapi.IN_HEADER, type=openapi.TYPE_STRING, required=True, description="language, for language support"),
        openapi.Parameter(
            name='featureId',
            required=False,
            default="6114c703f133370018bcc05b",
            in_=openapi.IN_QUERY,
            type=openapi.TYPE_STRING,
            description="featureId of perticular niche to display all the artists related."
        ),
        openapi.Parameter(
            name='search',
            required=False,
            default="",
            in_=openapi.IN_QUERY,
            type=openapi.TYPE_STRING,
            description="search query related to niche or the artist names."
        ),
        openapi.Parameter(
            name='from',
            default="0",
            in_=openapi.IN_QUERY,
            type=openapi.TYPE_STRING,
            description="starting data out of response penCount."
        ),
        openapi.Parameter(
            name='to',
            default="20",
            in_=openapi.IN_QUERY,
            type=openapi.TYPE_STRING,
            description="ending data out of response penCount."
        )
    ]

APIDocObj = APIDOC()