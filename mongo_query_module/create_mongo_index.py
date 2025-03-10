from search_api.settings import db


def create_index_function():
    # ========================================seller rating review=====================================================
    db.sellerReviewRatings.create_index(
        [
            ("sellerId", 1),
            ("rating", 1),
            ("status", 1)
        ]
    )
    db.sellerReviewRatings.create_index(
        [
            ("userId", 1),
            ("sellerId", 1),
            ("orderId", 1),
            ("attributeId", 1)
        ]
    )
    # ========================================cannabis product =======================================================
    db.cannabisProductType.create_index(
        [
            ("_id", 1),
            ("status", 1)
        ]
    )
    # ========================================child product index=======================================================
    db.childProducts.create_index(
        [
            ("parentProductId", 1),
            ("storeId", 1),
            ("status", 1)
        ]
    )
    db.childProducts.create_index(
        [
            ("units.unitId", 1),
            ("storeId", 1),
            ("status", 1)
        ]
    )
    db.childProducts.create_index(
        [
            ("storeId", 1),
            ("status", 1),
            ("linkedAttributeCategory.categoryId", 1)
        ]
    )
    db.childProducts.create_index(
        [
            ("parentProductId", 1),
            ("storeId", 1),
            ("status", 1),
            ("units.unitSizeGroupValue.en", 1),
            ("units.unitId", 1)
        ]
    )
    db.childProducts.create_index(
        [
            ("parentProductId", 1),
            ("storeId", 1),
            ("units.unitId", 1)
        ]
    )
    db.childProducts.create_index(
        [
            ("parentProductId", 1),
            ("storeId", 1),
            ("status", 1)
        ]
    )
    db.childProducts.create_index(
        [
            ("parentProductId", 1),
            ("_id", 1),
            ("status", 1)
        ]
    )
    db.childProducts.create_index(
        [
            ("storeId", 1),
            ("status", 1),
            ("storeCategoryId", 1)
        ]
    )
    db.childProducts.create_index(
        [
            ("units.unitName.en", 1),
            ("storeId", 1),
            ("status", 1),
            ("storeCategoryId", 1)
        ]
    )
    db.childProducts.create_index(
        [
            ("categoryList.parentCategory.childCategory.categoryId", 1),
            ("status", 1)
        ]
    )
    db.childProducts.create_index(
        [
            ("offer.offerId", 1),
            ("offer.status", 1)
        ]
    )
    db.childProducts.create_index(
        [
            ("status", 1),
            ("units.isPrimary", 1),
            ("storeId", 1),
            ("pName.en", 1)
        ]
    )
    db.childProducts.create_index(
        [
            ("_id", 1),
            ("status", 1),
            ("storeId", 1)
        ]
    )
    db.childProducts.create_index(
        [
            ("categoryList.parentCategory.childCategory.categoryId", 1),
            ("status", 1),
            ("storeId", 1)
        ]
    )
    db.childProducts.create_index(
        [
            ("parentProductId", 1),
            ("status", 1)
        ]
    )
    db.childProducts.create_index(
        [
            ("parentProductId", 1),
            ("storeId", 1)
        ]
    )
    db.childProducts.create_index(
        [
            ("storeId", 1),
            ("_id", 1)
        ]
    )
    db.childProducts.create_index(
        [
            ("status", 1),
            ("storeCategoryId", 1),
            ("units.isPrimary", 1),
            ("_id", 1),
            ("storeId", 1)
        ]
    )
    # ==============================================banner===============================================================
    db.banner.create_index(
        [
            ("status", 1),
            ("storeCategoryId", 1)
        ]
    )
    db.banner.create_index(
        [
            ("data.id", 1),
            ("status", 1),
            ("storeCategoryId", 1)
        ]
    )
    db.banner.create_index(
        [
            ("zones.zoneId", 1),
            ("status", 1),
            ("storeCategoryId", 1)
        ]
    )
    # ==============================================category=============================================================
    db.category.create_index(
        [
            ("status", 1),
            ("storeCategory.storeCategoryId", 1),
            ("storeId", 1)
        ]
    )
    db.category.create_index(
        [
            ("categoryName.en", 1),
            ("status", 1),
            ("parentId", 1)
        ]
    )
    db.category.create_index(
        [
            ("status", 1),
            ("parentId", 1)
        ]
    )
    db.category.create_index(
        [
            ("status", 1)
        ]
    )
    db.category.create_index(
        [
            ("categoryName", 1),
            ("status", 1)
        ]
    )
    db.category.create_index(
        [
            ("categoryName", 1),
            ("parentId", 1)
        ]
    )
    db.category.create_index(
        [
            ("_id", 1),
            ("attributeGroupData.AttributeList.attributeId", 1)
        ]
    )
    db.category.create_index(
        [
            ("status", 1),
            ("parentId", 1),
            ("storeId", 1),
            ("storeid", 1)
        ]
    )
    db.category.create_index(
        [
            ("categoryName.en", 1),
            ("status", 1),
            ("storeId", 1),
            ("storeid", 1)
        ]
    )
    db.category.create_index(
        [
            ("categoryName.en", 1),
            ("storeId", 1),
            ("storeid", 1)
        ]
    )
    db.category.create_index(
        [
            ("_id", 1),
            ("storeCategory.storeCategoryId", 1),
            ("status", 1),
            ("parentId", 1)
        ]
    )
    db.category.create_index(
        [
            ("_id", 1),
            ("status", 1),
            ("parentId", 1)
        ]
    )
    db.category.create_index(
        [
            ("attributeGroupData.AttributeList.attributeId", 1),
            ("status", 1),
            ("_id", 1)
        ]
    )
    db.category.create_index(
        [
            ("_id", 1),
            ("parentId", 1),
            ("storeid", 1)
        ]
    )
    db.category.create_index(
        [
            ("categoryName.en", 1),
            ("storeCategory.storeCategoryId", 1)
        ]
    )
    db.category.create_index(
        [
            ("categoryName.en", 1)
        ]
    )
    # ==========================================offer====================================================================
    db.offers.create_index(
        [
            ("status", 1),
            ("_id", 1),
            ("storeid", 1),
            ("storeId", 1)
        ]
    )
    db.offers.create_index(
        [
            ("storeId", 1),
            ("status", 1)
        ]
    )
    db.offers.create_index(
        [
            ("status", 1),
            ("_id", 1),
            ("storeId", 1)
        ]
    )
    db.offers.create_index(
        [
            ("status", 1),
            ("_id", 1)
        ]
    )
    db.offers.create_index(
        [
            ("storeId", 1),
            ("_id", 1)
        ]
    )
    # ==========================================products=================================================================
    db.products.create_index(
        [
            ("units.attributes.attrlist.attributeId", 1)
        ]
    )
    db.products.create_index(
        [
            ("childProducts.attributes.attrlist.attributeId", 1)
        ]
    )
    db.products.create_index(
        [
            ("status", 1),
            ("childProducts.suppliers.id", 1),
            ("childProducts.unitId", 1),
            ("storeCategoryId", 1)
        ]
    )
    db.products.create_index(
        [
            ("units.suppliers.productId", 1),
            ("units.unitId", 1)
        ]
    )
    # ==========================================reviewRatings============================================================
    db.reviewRatings.create_index(
        [
            ("productId", 1),
            ("sellerId", 1),
            ("attributeId", 1)
        ]
    )
    db.reviewRatings.create_index(
        [
            ("productId", 1),
            ("rating", 1),
            ("status", 1)
        ]
    )
    db.reviewRatings.create_index(
        [
            ("productId", 1),
            ("userId", 1),
            ("status", 1),
            ("orderId", 1)
        ]
    )
    db.reviewRatings.create_index(
        [
            ("productId", 1),
            ("userId", 1),
            ("status", 1),
            ("orderId", 1),
            ("childProductId", 1)
        ]
    )
    db.reviewRatings.create_index(
        [
            ("productId", 1),
            ("userId", 1),
            ("status", 1),
            ("orderId", 1),
            ("attributeId", 1)
        ]
    )
    db.reviewRatings.create_index(
        [
            ("productId", 1),
            ("userId", 1),
            ("status", 1),
            ("orderId", 1),
            ("attributeId", 1),
            ("childProductId", 1),
        ]
    )

    db.reviewRatings.create_index(
        [
            ("productId", 1),
            ("userId", 1),
            ("orderId", 1)
        ]
    )
    db.reviewRatings.create_index(
        [
            ("productId", 1),
            ("userId", 1),
            ("orderId", 1),
            ("childProductId", 1)
        ]
    )
    db.reviewRatings.create_index(
        [
            ("productId", 1),
            ("userId", 1),
            ("orderId", 1),
            ("attributeId", 1)
        ]
    )
    db.reviewRatings.create_index(
        [
            ("productId", 1),
            ("userId", 1),
            ("orderId", 1),
            ("childProductId", 1),
            ("attributeId", 1)
        ]
    )
    db.reviewRatings.create_index(
        [
            ("productId", 1),
            ("userId", 1),
            ("status", 1),
            ("orderId", 1),
            ("attributeId", 1),
            ("childProductId", 1),
        ]
    )
    db.reviewRatings.create_index(
        [
            ("status", 1)
        ]
    )
    db.reviewRatings.create_index(
        [
            ("productId", 1),
            ("status", 1)
        ]
    )
    db.reviewRatings.create_index(
        [
            ("productId", 1)
        ]
    )
    # ==================================================zones===========================================================
    db.zones.create_index(
        [
            ("status", 1),
            ("polygons", 1)
        ]
    )
    db.zones.create_index(
        [
            ("status", 1),
            ("storeId", 1),
            ("polygons", 1)
        ]
    )
    db.zones.create_index(
        [
            ("status", 1),
            ("city_ID", 1)
        ]
    )
    # ==================================================zones===========================================================
    db.stores.create_index(
        [
            ("serviceZones.zoneId", 1),
            ("storeFrontTypeId", 1),
            ("status", 1)
        ]
    )
    db.stores.create_index(
        [
            ("serviceZones.zoneId", 1),
            ("categoryId", 1)
        ]
    )
    db.stores.create_index(
        [
            ("serviceZones.zoneId", 1),
            ("status", 1),
            ("categoryId", 1)
        ]
    )
    db.stores.create_index(
        [
            ("serviceZones.zoneId", 1),
            ("status", 1),
            ("_id", 1)
        ]
    )
    # ==================================================posts===========================================================
    db.posts.create_index(
        [
            ("postStatus", 1),
            ("productData", 1)
        ]
    )
    # ==================================================store orders====================================================
    db.posts.create_index(
        [
            ("status.status", 1),
            ("createdBy.userId", 1),
            ("storeCategoryId", 1)
        ]
    )
    # ==================================================currency========================================================
    db.currencies.create_index(
        [
            ("currencyCode", 1)
        ]
    )
    # ==================================================notification====================================================
    db.notificationLogs.create_index(
        [
            ("app_name", 1),
            ("userid", 1)
        ]
    )
    db.notificationLogs.create_index(
        [
            ("app_name", 1),
            ("isSeen", 1),
            ("userid", 1)
        ]
    )
    db.notificationLogs.create_index(
        [
            ("app_name", 1),
            ("userid", 1),
            ("store_category_id", 1)
        ]
    )
    db.notificationLogs.create_index(
        [
            ("app_name", 1),
            ("isSeen", 1),
            ("userid", 1),
            ("store_category_id", 1)
        ]
    )
    # db.notificationLogs.create_index(
    #     [
    #         ("userid", 1),
    #         ("notificationMongoId", 1),
    #         ("createdtimestamp", 1)
    #     ]
    # )
    # ==========================================mega menu========================================================
    db.ecomMegaMenu.create_index(
        [
            ("storeCategoryId", 1)
        ]
    )
    db.ecomMegaMenu.create_index(
        [
            ("storeCategoryId", 1),
            ("storeId", 1)
        ]
    )
    db.ecomMegaMenuColumn.create_index(
        [
            ("menuId", 1)
        ]
    )
    db.ecomMegaMenuRows.create_index(
        [
            ("columnId", 1)
        ]
    )
    # =============================================product attributes=============================================
    db.productAttribute.create_index(
        [
            ("_id", 1),
            ("searchable", 1),
            ("attriubteType", 1)
        ]
    )
    db.productAttribute.create_index(
        [
            ("_id", 1),
            ("linkedtounit", 1),
            ("status", 1)
        ]
    )
    # =============================================zone alerts=============================================
    db.zoneAlert.create_index(
        [
            ("storeCategories.categoryId", 1),
            ("status", 1),
            ("zoneId", 1),
            ("startsTimeStemp", 1),
            ("endTimeStemp", 1)
        ]
    )
    # =============================================home page=============================================
    db.ecomHomePage.create_index(
        [
            ("storeCategoryId", 1),
            ("contentType", 1),
            ("status", 1),
            ("testMode", 1)
        ]
    )
    db.ecomHomePage.create_index(
        [
            ("storeCategoryId", 1),
            ("contentType", 1)
        ]
    )
    # =============================================mega menu=============================================
    db.ecomMegaMenuRows.create_index(
        [
            ("columnId", 1),
            ("level", 1)
        ]
    )
    db.ecomMegaMenuRows.create_index(
        [
            ("columnId", 1)
        ]
    )
    db.ecomMegaMenuColumn.create_index(
        [
            ("menuId", 1)
        ]
    )
    db.ecomMegaMenu.create_index(
        [
            ("storeCategoryId", 1),
            ("status", 1)
        ]
    )
    db.ecomMegaMenu.create_index(
        [
            ("storeCategoryId", 1),
            ("storeId", 1),
            ("status", 1)
        ]
    )
    # =============================================brands=============================================
    db.brands.create_index(
        [
            ("storeCategoryId", 1),
            ("name", 1),
            ("status", 1)
        ]
    )
    # =============================================brands=============================================
    db.brands.create_index(
        [
            ("storeCategoryId", 1),
            ("name.en", 1),
            ("status", 1)
        ]
    )
    # =============================================shopping list=============================================
    db.userShoppingList.create_index(
        [
            ("userId", 1),
            ("products.centralProductId", 1),
            ("products.childProductId", 1)
        ]
    )
    # =============================================size chart=============================================
    db.sizeGroup.create_index(
        [
            ("storeCategoryId", 1),
            ("categories.categoryName.en", 1),
            ("status", 1)
        ]
    )
    # ==============================================managers==============================================
    db.managers.create_index(
        [
            ("_id", 1),
            ("linkedWith", 1)
        ]
    )
    # ==============================================reviewRatings==========================================
    db.reviewRatings.create_index(
        [
            ("productId", 1)
        ]
    )
    db.reviewRatings.create_index(
        [
            ("productId", 1),
            ("status", 1)
        ]
    )
    # ==============================================driverRoasterDaily==========================================
    db.driverRoasterDaily.create_index(
        [
            ("status", 1),
            ("startDateTime", 1),
            ("roasterStoreId", 1),
            ("typeId", 1),
            ("jobs", 1),
            ("noOfJob", 1)
        ]
    )
    # ==============================================storeOrder==========================================
    db.storeOrder.create_index(
        [
            ("customerId", 1),
            ("status.status", 1)
        ]
    )
    db.storeOrder.create_index(
        [
            ("storeOrderId", 1),
            ("customerDetails.id", 1)
        ]
    )
    db.storeOrder.create_index(
        [
            ("customerId", 1),
            ("products.centralProductId", 1),
            ("status.status", 1)
        ]
    )
    db.storeOrder.create_index(
        [
            ("customerId", 1),
            ("storeOrderId", 1),
            ("products.centralProductId", 1),
            ("status.status", 1)
        ]
    )
    db.storeOrder.create_index(
        [
            ("customerId", 1),
            ("products.centralProductId", 1),
            ("status.status", 1)
        ]
    )
    db.storeOrder.create_index(
        [
            ("customerId", 1),
            ("storeOrderId", 1),
            ("status.status", 1)
        ]
    )
    db.storeOrder.create_index(
        [
            ("customerId", 1),
            ("products.productId", 1),
            ("products.centralProductId", 1),
            ("status.status", 1)
        ]
    )
    db.storeOrder.create_index(
        [
            ("customerId", 1),
            ("products.productId", 1),
            ("storeOrderId", 1),
            ("status.status", 1)
        ]
    )
    db.storeOrder.create_index(
        [
            ("customerId", 1),
            ("products.productId", 1),
            ("storeOrderId", 1),
            ("products.centralProductId", 1),
            ("status.status", 1)
        ]
    )
    db.storeOrder.create_index(
        [
            ("products.centralProductId", 1),
            ("customerDetails.id", 1)
        ]
    )
    db.storeOrder.create_index(
        [
            ("products.centralProductId", 1),
            ("customerDetails.id", 1),
            ("products.productId", 1)
        ]
    )
    # ==============================================ratingParams==========================================
    db.ratingParams.create_index(
        [
            ("_id", 1),
            ("associated", 1)
        ]
    )
    # ==============================================driverRatingReview===================================
    db.driverRatingReview.create_index(
        [
            ("userId", 1),
            ("driverId", 1),
            ("orderId", 1),
            ("attributeId", 1)
        ]
    )
    db.driverRatingReview.create_index(
        [
            ("userId", 1),
            ("driverId", 1),
            ("rating", 1),
            ("status", 1)
        ]
    )
    db.driverRatingReview.create_index(
        [
            ("driverId", 1),
            ("rating", 1)
        ]
    )
    db.cities.create_index(
        [
            ("storeCategory.storeCategoryId", 1),
            ("_id", 1)
        ]
    )
    # ========== for user recent search=========================
    db.userRecentSearch.create_index(
        [
            ("userid", 1),
            ("store_category_id", 1)
        ]
    )
    db.userRecentSearch.create_index(
        [
            ("userid", 1),
            ("store_category_id", 1),
            ("storeid", 1)
        ]
    )
    # ========== for user recent view=========================
    db.userRecentView.create_index(
        [
            ("store_category_id", 1),
            ("userid", 1),
            ("createdtimestamp", 1)
        ]
    )
    print("index successfully created")
    return True
