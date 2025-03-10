def language_change(key, language):
    language_change_function = {
        "Table Reservation":{
            'en': "Table Reservation",
            'es': "Reserva de mesa",
            "ar": "حجز الطاولة"
        },
        "Opens Next At": {
            'en': "Opens Next At",
            'es': "Se abre siguiente en",
            "ar": "يفتح التالي عند"
        },
        "Opens Tomorrow At": {
            'en': "Opens Tomorrow At",
            'ar': "يفتح غدًا عند",
            'es': "abre mañana a las",
        },
        "Temporarily Closed": {
            'en': "Temporarily Closed",
            "ar": "مغلق مؤقتا",
            "es": "Cerrado temporalmente"
        },
        "Recommended": {
            "en": "Recommended",
            'ar': "مُستَحسَن",
            'es': "Recomendado",
        },
        "Favorites": {
            "en": "Favorites",
            'ar': "المفضلة",
            'es': "Favoritos",
        },
        "We don’t operate in your region at this moment , please contact our support team for further queries.":{
            "en": "We don’t operate in your region at this moment , please contact our support team for further queries.",
            'ar': "نحن لا نعمل في منطقتك في الوقت الحالي ، يرجى الاتصال بفريق الدعم لمزيد من الاستفسارات.",
            'es': "No operamos en su región en este momento, comuníquese con nuestro equipo de soporte para más consultas.",

        },
        "Cuisines":{
            "en":"Cuisines",
            "ar":"المطابخ",
            "es":"Cocinas"
        },
        "STORE TYPES":{
            "en":"STORE TYPES",
            "ar":"أنواع المتاجر",
            "es":"TIPOS DE TIENDA"
        },
        "Self Pickup":{
            "en":"Self Pickup",
            "ar":"النفس التقاط",
            "es":"Recogerse"
        },
        "Delivery":{
            "en":"Delivery",
            "ar":"توصيل",
            "es":"Entrega"
        },
        "Order Type":{
            "en":"Order Type",
            "ar":"نوع الطلب",
            "es":"Tipo de orden"
        },
        "Popularity":{
            "en":"Popularity",
            "ar":"شعبية",
            "es":"Popularidad"
        },
        "Ratings High to Low":{
            "en":"Ratings High to Low",
            "ar":"التقييمات من الأعلى إلى الأقل",
            "es":"Calificaciones de alta a baja"
        },
        "Distance: Closer to Far":{
            "en":"Distance: Closer to Far",
            "ar": "المسافة: أقرب إلى أقصى",
            "es":"Distancia: más cerca de lejos"
        },
        "Distance: Far to Closer":{
            "en":"Distance: Far to Closer",
            "ar":"المسافة: من بعيد إلى أقرب",
            "es":"Distancia: de lejos a más cerca"
        },
        "Cost: Low to High":{
            "en":"Cost: Low to High",
            "ar":"التكلفة: من الأقل إلى الأعلى",
            "es":"Costo: Baja a Alta"
        },
        "Cost: High to Low":{
            "en":"Cost: High to Low",
            "ar":"التكلفة: من الأعلى إلى الأقل",
            "es":"Costo: Alta a Baja"
        },
        "Sort":{
            "en":"Sort",
            "ar":"نوع",
            "es":"clasificar"
        },
        "Ratings":{
            "en":"Ratings",
            'ar':"التقييمات",
            "es":"Calificaciones"
        },
        "Cost for two":{
            "en":"Cost for two",
            "ar":"التكلفة لشخصين",
            "es":"Costo para dos"
        }
    }

    # Check if the key exists in the dictionary
    return language_change_function[key][language] if key in language_change_function and language in \
                                                      language_change_function[key] else language_change_function[key][
        'en']
