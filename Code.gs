var SHOP_DOMAIN = "pff-premium-store.myshopify.com";
const ACCESS_TOKEN = PropertiesService.getScriptProperties().getProperty('SHOPIFY_ACCESS_TOKEN')
var SHEET_ID = "1ScwvIVZLisTSvrFxmUj51TSJ2XERz8mxfKP6jvY7XQw";
var LOCATION_ID = "69861605557";
var API_VERSION = "2025-10"; // Use a stable version (2025-10 implies future/unstable)

// ---------------------------------------------------------
// WEBHOOK & HTTP HANDLERS
// ---------------------------------------------------------

// Handle Webhooks from Shopify (New Order / Update Order)
function doPost(e) {
  try {
    if (!e || !e.postData || !e.postData.contents) return ContentService.createTextOutput("No data");
    var jsonString = e.postData.contents;
    var data = JSON.parse(jsonString);
    var topic = e.parameter.topic || "orders/create";
    // You can pass ?topic=orders/update in webhook URL

    // Process the webhook data sync to Sheet safely
    syncOrderToSheet(data);
    
    return ContentService.createTextOutput("Webhook Received");
  } catch (err) {
    console.error("Webhook Error: " + err.toString());
    return ContentService.createTextOutput("Error");
  }
}

function doGet(e) {
  var action = e.parameter.action;
  if (!action) {
    return HtmlService.createTemplateFromFile('index')
      .evaluate()
      .setTitle('PFF Premium Orders')
      .addMetaTag('viewport', 'width=device-width, initial-scale=1, maximum-scale=1, user-scalable=0')
      .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
  }

  if (action === "getOrders") return handleGetOrders(e);
  if (action === "checkCustomer") return handleCheckCustomer(e);
  if (action === "saveOrder") return handleSaveOrder(e);
  if (action === "getSheetOrders") return handleGetSheetOrders(e);
  if (action === "updateSheetOrder") return handleUpdateSheetOrder(e);
  if (action === "getOrderImages") return handleGetOrderImages(e);
  if (action === "updateCustomerOnly") return handleUpdateCustomerOnly(e);
  if (action === "getLatestOrderId") return handleGetLatestOrderId(e); // NEW: Notification checking
  
  // --- FULFILLMENT ACTIONS ---
  if (action === "getFulfillmentOrders") return handleGetFulfillmentOrders(e);
  if (action === "fulfillOrder") return handleFulfillOrder(e);
  
  // --- SHOP ACTIONS ---
  if (action === "markShopifyPaid") return handleMarkShopifyPaid(e);
  if (action === "cancelShopifyOrder") return handleCancelShopifyOrder(e);
  if (action === "restockItem") return handleRestockItem(e);
  
  // --- NEW EDIT ACTIONS ---
  if (action === "searchProducts") return handleSearchProducts(e);
  if (action === "editShopifyOrder") return handleEditShopifyOrder(e);
  
  // --- UPDATE ORDER CUSTOMER DETAILS ---
  if (action === "updateOrderCustomer") return handleUpdateOrderCustomer(e);
  
  // --- INVENTORY ACTIONS ---
  if (action === "getProductVariants") return handleGetProductVariants(e);
  if (action === "updateInventorySet") return handleUpdateInventorySet(e);
}
// ---------------------------------------------------------
// NEW: GET LATEST ORDER FOR NOTIFICATION
// ---------------------------------------------------------
function handleGetLatestOrderId(e) {
  try {
    // Shudhu 1ta order nibo jate Shopify rate limit problem na kore
    var endpoint = "orders.json?limit=1&status=any&fields=id,order_number,total_price";
    var response = fetchShopifySafe(endpoint, "get");
    var json = JSON.parse(response.getContentText());
    
    if (json.orders && json.orders.length > 0) {
      return sendJSON({status: "success", latest: json.orders[0].order_number, amount: json.orders[0].total_price});
    }
    return sendJSON({status: "error", message: "No orders found"});
  } catch(err) { 
    return sendJSON({status: "error", message: err.toString()}); 
  }
}
// ---------------------------------------------------------
// ANTI-BLOCKING SHOPIFY FETCHER
// ---------------------------------------------------------
function fetchShopifySafe(endpoint, method, payload) {
  var url = "https://" + SHOP_DOMAIN + "/admin/api/" + API_VERSION + "/" + endpoint;
  var options = {
    "method": method || "get",
    "headers": { 
      "X-Shopify-Access-Token": ACCESS_TOKEN,
      "Content-Type": "application/json"
    },
    "muteHttpExceptions": true
  };
  if (payload) options.payload = JSON.stringify(payload);

  var maxRetries = 3;
  for (var i = 0; i < maxRetries; i++) {
    var response = UrlFetchApp.fetch(url, options);
    var code = response.getResponseCode();
    
    // Check for Rate Limit (429) to prevent blocking
    if (code === 429) {
      var retryAfter = response.getHeaders()['Retry-After'];
      var sleepTime = retryAfter ? parseFloat(retryAfter) * 1000 : 2000;
      Utilities.sleep(sleepTime + 500); // Add slight buffer
      continue; 
    }
    return response; 
  }
  throw new Error("Shopify API Rate Limit Exceeded after retries.");
}

function shopifyGraphQL(query, variables) {
  var url = "graphql.json";
  var payload = { query: query, variables: variables };
  try {
    var res = fetchShopifySafe(url, "post", payload);
    return JSON.parse(res.getContentText());
  } catch(e) {
    return { errors: [{ message: e.toString() }] };
  }
}

// ---------------------------------------------------------
// HELPER: SYNC WEBHOOK DATA TO SHEET
// ---------------------------------------------------------
function syncOrderToSheet(orderData) {
  var lock = LockService.getScriptLock();
  try {
    lock.waitLock(10000); // Wait up to 10 seconds to avoid collisions
  } catch (e) {
    console.error("Could not obtain lock after 10 seconds.");
    return;
  }

  try {
    var ss = SpreadsheetApp.openById(SHEET_ID);
    var sheet = ss.getSheetByName("Orders");
    var data = sheet.getDataRange().getValues();
    var orderId = String(orderData.order_number);
    var rowIndex = -1;

    // Check if order already exists
    for (var i = 1; i < data.length; i++) {
      if (String(data[i][1]).replace(/'/g, "") === orderId) {
        rowIndex = i + 1;
        break;
      }
    }

    // Format Data
    var date = new Date(orderData.created_at).toISOString().slice(0, 10);
    var name = orderData.shipping_address ? 
      (orderData.shipping_address.first_name + " " + orderData.shipping_address.last_name) : 
      (orderData.customer ? orderData.customer.first_name + " " + orderData.customer.last_name : "No Name");
    var phone = String((orderData.shipping_address ? orderData.shipping_address.phone : (orderData.customer ? orderData.customer.phone : "")) || "")
      .replace(/[^0-9]/g, "")
      .replace(/^88/, "");
    var address = orderData.shipping_address ? 
      [orderData.shipping_address.address1, orderData.shipping_address.address2, orderData.shipping_address.city].filter(Boolean).join(", ") : "";
    
    // PRICE LOGIC: Prioritize current_total_price
    var amount = orderData.total_price;
    if (orderData.current_total_price !== undefined && orderData.current_total_price !== null) {
       amount = orderData.current_total_price;
    }

    if (rowIndex === -1) {
      sheet.appendRow([date, "'" + orderId, name, "'" + phone, address, amount, "", "Pending", ""]);
      SpreadsheetApp.flush();
    } else {
      sheet.getRange(rowIndex, 6).setValue(amount);
      SpreadsheetApp.flush();
    }
  } catch (err) {
    console.error("Sync Error: " + err.toString());
  } finally {
    lock.releaseLock();
  }
}

// ---------------------------------------------------------
// NEW: GET VARIANTS & INVENTORY FOR PREVIEW
// ---------------------------------------------------------
function handleGetProductVariants(e) {
  var pId = e.parameter.productId;
  if (!pId) return sendJSON({status: "error", message: "Product ID missing"});
  
  var MAIN_LOC = "69861605557";
  var DHK_LOC = "115051364533";
  var BNN_LOC = "115051397301";
  var UTR_LOC = "115051430069";
  var ALL_LOCS = MAIN_LOC + "," + DHK_LOC + "," + BNN_LOC + "," + UTR_LOC;

  try {
    var query = `query {
      product(id: "gid://shopify/Product/${pId}") {
        variants(first: 50) {
          edges {
            node {
              id
              title
              sku
              inventoryItem { id }
            }
          }
        }
      }
    }`;
    var json = shopifyGraphQL(query, {});
    if (json.errors || !json.data || !json.data.product) {
      return sendJSON({status: "error", message: "Failed to fetch variant details via GraphQL"});
    }

    var variants = json.data.product.variants.edges;
    var invItemIds = variants.map(function(edge) { return edge.node.inventoryItem.id.split("/").pop(); });
    
    var invRes = fetchShopifySafe("inventory_levels.json?location_ids=" + ALL_LOCS + "&inventory_item_ids=" + invItemIds.join(","), "get");
    var invJson = JSON.parse(invRes.getContentText());
    
    var levelsMap = {};
    if (invJson.inventory_levels) {
        invJson.inventory_levels.forEach(function(lvl) {
            if (!levelsMap[lvl.inventory_item_id]) levelsMap[lvl.inventory_item_id] = {};
            levelsMap[lvl.inventory_item_id][lvl.location_id] = lvl.available;
        });
    }

    var result = variants.map(function(edge) {
        var node = edge.node;
        var invId = node.inventoryItem.id.split("/").pop();
        var locData = levelsMap[invId] || {};
        return {
            id: node.id.split("/").pop(),
            title: node.title,
            sku: node.sku || "",
            inventory_item_id: invId,
            qty: locData[MAIN_LOC] !== undefined ? locData[MAIN_LOC] : 0,
            dhanmondi: locData[DHK_LOC] !== undefined ? locData[DHK_LOC] : 0,
            banani: locData[BNN_LOC] !== undefined ? locData[BNN_LOC] : 0,
            uttara: locData[UTR_LOC] !== undefined ? locData[UTR_LOC] : 0
        };
    });
    return sendJSON({status: "success", variants: result});
    
  } catch (err) {
    return sendJSON({status: "error", message: err.toString()});
  }
}

function handleUpdateInventorySet(e) {
  var p = e.parameter;
  var invItemId = p.invItemId;
  var locationId = p.locationId;
  var qty = parseInt(p.qty);
  
  if (!invItemId || !locationId) return sendJSON({status: "error", message: "Item ID or Location ID missing"});
  
  try {
    fetchShopifySafe("inventory_levels/set.json", "post", {
        "location_id": locationId,
        "inventory_item_id": invItemId,
        "available": qty
    });

    return sendJSON({status: "success", message: "Updated successfully"});
  } catch(err) {
    return sendJSON({status: "error", message: err.toString()});
  }
}

// ---------------------------------------------------------
// 1. GET ORDERS (Shopify + Saved Data)
// ---------------------------------------------------------
function handleGetOrders(e) {
  try {
    var savedDataMap = getSavedOrderDetailsMap();
    var limit = 50; 
    var params = e.parameter;
    
    var endpoint = "orders.json?limit=" + limit;
    var userStatus = params.status || "any";
    
    if (userStatus !== "any" && ["pending", "paid", "void", "product_entry", "courier"].indexOf(userStatus) !== -1) {
       var filteredIds = [];
       for (var id in savedDataMap) {
          var status = String(savedDataMap[id].status || "").toLowerCase().trim();
          var cleanStatus = status.replace("paid", "").replace("void", "").replace("product entry", "").replace("courier", "").replace(/[^a-z0-9]/g, "");
          
          if (userStatus === "pending") {
             if (status === "" || status.includes("pending") || cleanStatus.length > 0) filteredIds.push(id);
          }
          else if (userStatus === "paid" && status.includes("paid")) filteredIds.push(id);
          else if (userStatus === "void" && status.includes("void")) filteredIds.push(id);
          else if (userStatus === "product_entry" && status.includes("product entry")) filteredIds.push(id);
          else if (userStatus === "courier" && status.includes("courier")) filteredIds.push(id);
       }

       if (filteredIds.length > 0) {
          var limitedIds = filteredIds.slice(0, 60); 
          var queryParts = limitedIds.map(function(num) { return "name:" + num; });
          endpoint += "&status=any&query=" + encodeURIComponent(queryParts.join(" OR "));
       } else {
          return sendJSON({status: "success", orders: [], images: {}, savedIds: [], savedDetails: {}, entryAmount: 0});
       }
    } else {
       if (["paid", "pending", "voided", "authorized", "partially_paid", "refunded"].indexOf(userStatus) !== -1) {
          endpoint += "&status=any&financial_status=" + userStatus;
       } else {
          endpoint += "&status=" + (userStatus === 'any' ? 'any' : userStatus);
       }
    }

    if (params.search) {
      var q = params.search.trim();
      var qLower = q.toLowerCase();
      var searchTerms = [];
      for (var id in savedDataMap) {
        var s = savedDataMap[id];
        var sName = String(s.name || "").toLowerCase();
        var sPhone = String(s.phone || "").replace(/[^0-9]/g, ""); 
        var qClean = qLower.replace(/[^0-9]/g, "");
        var sInvoice = String(s.invoice || "").toLowerCase();

        if (sName.indexOf(qLower) > -1 || (qClean.length > 5 && sPhone.indexOf(qClean) > -1) || sInvoice.indexOf(qLower) > -1) {
            searchTerms.push("name:" + id);
        }
      }
      var rawQuery = q;
      if (/^\d+$/.test(rawQuery) && rawQuery.length < 10) rawQuery = "#" + rawQuery; 
      searchTerms.push(rawQuery);
      var uniqueTerms = [...new Set(searchTerms)].slice(0, 25);
      endpoint += "&status=any&query=" + encodeURIComponent(uniqueTerms.join(" OR "));
    }

    if (params.created_at_min) endpoint += "&created_at_min=" + params.created_at_min + "T00:00:00";
    if (params.created_at_max) endpoint += "&created_at_max=" + params.created_at_max + "T23:59:59";
    if (params.dateFrom) endpoint += "&created_at_min=" + params.dateFrom + "T00:00:00";
    if (params.dateTo) endpoint += "&created_at_max=" + params.dateTo + "T23:59:59";
    if (params.page_info) endpoint = "orders.json?limit=" + limit + "&page_info=" + params.page_info;
    
    var response = fetchShopifySafe(endpoint, "get");
    var json = JSON.parse(response.getContentText());
    var nextCursor = "", prevCursor = "";
    if (response.getAllHeaders()['Link']) {
      var links = response.getAllHeaders()['Link'].split(',');
      links.forEach(function(link) {
        if (link.indexOf('rel="next"') > -1) nextCursor = link.match(/page_info=([^>&]+)/)[1];
        if (link.indexOf('rel="previous"') > -1) prevCursor = link.match(/page_info=([^>&]+)/)[1];
      });
    }

    var orders = json.orders || [];
    if (["pending", "paid", "void", "product_entry", "courier"].indexOf(userStatus) !== -1) {
      orders = orders.filter(function(order) { return savedDataMap[String(order.order_number)] !== undefined; });
    }
    
    // FAST ASYNC IMAGE FETCHING
    var imagesMap = fetchImagesForOrders(orders);
    
    var entryAmountTotal = 0;
    for (var id in savedDataMap) {
        if (savedDataMap[id].status && savedDataMap[id].status.toLowerCase().indexOf("product entry") !== -1) {
            var val = parseFloat(String(savedDataMap[id].amount).replace(/[^0-9.-]+/g,""));
            if (!isNaN(val)) entryAmountTotal += val;
        }
    }

    return sendJSON({
      status: "success", orders: orders, images: imagesMap, savedIds: Object.keys(savedDataMap), savedDetails: savedDataMap, 
      entryAmount: entryAmountTotal, nextPage: nextCursor, prevPage: prevCursor
    });
  } catch (err) { return sendJSON({status: "error", message: err.toString()}); }
}

// ---------------------------------------------------------
// 2. SHEET ORDERS LOGIC (Order List Tab)
// ---------------------------------------------------------
function handleGetSheetOrders(e) {
  try {
    var ss = SpreadsheetApp.openById(SHEET_ID);
    var sheet = ss.getSheetByName("Orders");
    var data = sheet.getDataRange().getValues();
    var rows = data.slice(1);
    var orderList = [];
    for (var i = 0; i < rows.length; i++) {
      var r = rows[i];
      orderList.push({
        row: i + 2, date: r[0], id: String(r[1]).replace(/'/g, ""), name: r[2], number: String(r[3]), 
        address: r[4], amount: r[5], note: r[6], status: r[7], invoice: r[8]
      });
    }
    
    if (e.parameter.search) {
      var q = e.parameter.search.toLowerCase().trim();
      orderList = orderList.filter(function(o) {
        return String(o.id).toLowerCase().indexOf(q) > -1 || String(o.name).toLowerCase().indexOf(q) > -1 || String(o.number).toLowerCase().indexOf(q) > -1;
      });
    }

    orderList.reverse(); 

    if (e.parameter.export === 'true') { return sendJSON({status: "success", orders: orderList}); }

    var page = parseInt(e.parameter.page) || 1;
    var limit = 50;
    var offset = (page - 1) * limit;
    var pagedList = orderList.slice(offset, offset + limit);
    var hasMore = (offset + limit) < orderList.length;

    var orderIdsForImages = pagedList.map(function(o){ return o.id; });
    var imagesMap = {};
    if (orderIdsForImages.length > 0) { imagesMap = fetchImagesByOrderIds(orderIdsForImages); }
    
    return sendJSON({ status: "success", orders: pagedList, images: imagesMap, hasMore: hasMore, total: orderList.length });
  } catch (err) { return sendJSON({status: "error", message: err.toString()}); }
}

// ---------------------------------------------------------
// GRAPHQL SEARCH PRODUCTS
// ---------------------------------------------------------
function handleSearchProducts(e) {
  var q = e.parameter.q;
  if(!q) return sendJSON({status: "error", message: "Query missing"});
  try {
     var term = "*" + q + "*";
     var searchString = "title:" + term + " OR sku:" + term + " OR product_type:" + term;
     var gql = `query search($q: String!) { products(first: 10, query: $q) { edges { node { id title images(first: 1) { edges { node { url } } } variants(first: 5) { edges { node { id title sku price } } } } } } }`;
     var json = shopifyGraphQL(gql, { q: searchString });
     var results = [];
     if(json.data && json.data.products) {
        json.data.products.edges.forEach(function(edge){
            var p = edge.node; var pId = p.id.split("/").pop(); var img = (p.images.edges.length > 0) ? p.images.edges[0].node.url : "";
            p.variants.edges.forEach(function(vEdge){
               var v = vEdge.node; var vId = v.id.split("/").pop();
               results.push({ id: vId, product_id: pId, title: p.title, variant_title: v.title === 'Default Title' ? '' : v.title, sku: v.sku, price: v.price, image: img });
            });
        });
     }
     return sendJSON({status: "success", results: results});
  } catch(e) { return sendJSON({status: "error", message: e.toString()}); }
}

// ---------------------------------------------------------
// GRAPHQL EDIT SHOPIFY ORDER
// ---------------------------------------------------------
function handleEditShopifyOrder(e) {
  var p = e.parameter; var orderNumber = p.orderId;
  var additions = p.additions ? JSON.parse(p.additions) : []; var removals = p.removals ? JSON.parse(p.removals) : [];
  try {
     var realOrderId = findShopifyOrderId(orderNumber);
     if (!realOrderId) return sendJSON({status: "error", message: "Order #" + orderNumber + " not found"});
     var orderGid = "gid://shopify/Order/" + realOrderId;
     var beginQuery = `mutation beginEdit($id: ID!) { orderEditBegin(id: $id) { calculatedOrder { id lineItems(first:50) { edges { node { id variant { id legacyResourceId } } } } } userErrors { field message } } }`;
     var beginRes = shopifyGraphQL(beginQuery, { id: orderGid });
     if(beginRes.data.orderEditBegin.userErrors.length > 0) return sendJSON({status: "error", message: JSON.stringify(beginRes.data.orderEditBegin.userErrors)});
     var calcId = beginRes.data.orderEditBegin.calculatedOrder.id;
     var currentLines = beginRes.data.orderEditBegin.calculatedOrder.lineItems.edges;
     
     for(var i=0; i<removals.length; i++) {
        var rem = removals[i];
        var targetLine = null;
        for(var j=0; j<currentLines.length; j++) {
          var lineNode = currentLines[j].node;
          if(lineNode.variant && lineNode.variant.legacyResourceId && String(lineNode.variant.legacyResourceId) === String(rem.variantId)) { targetLine = lineNode; break; }
        }
        if(targetLine) {
          var remQuery = `mutation editQty($id: ID!, $lineItemId: ID!, $qty: Int!) { orderEditSetQuantity(id: $id, lineItemId: $lineItemId, quantity: $qty) { calculatedOrder { id } userErrors { field message } } }`;
          shopifyGraphQL(remQuery, { id: calcId, lineItemId: targetLine.id, qty: 0 });
        }
     }
     
     for(var j=0; j<additions.length; j++) {
        var add = additions[j];
        var varGid = "gid://shopify/ProductVariant/" + add.variantId;
        var addQuery = `mutation addVar($id: ID!, $variantId: ID!, $qty: Int!) { orderEditAddVariant(id: $id, variantId: $variantId, quantity: $qty) { calculatedOrder { id } userErrors { field message } } }`;
        shopifyGraphQL(addQuery, { id: calcId, variantId: varGid, qty: parseInt(add.qty) });
     }
     var commitQuery = `mutation commitEdit($id: ID!) { orderEditCommit(id: $id) { order { id } userErrors { field message } } }`;
     var commitRes = shopifyGraphQL(commitQuery, { id: calcId });
     if(commitRes.data.orderEditCommit.userErrors.length > 0) return sendJSON({status: "error", message: JSON.stringify(commitRes.data.orderEditCommit.userErrors)});
     return sendJSON({status: "success", message: "Order Updated"});
  } catch(err) { return sendJSON({status: "error", message: err.toString()}); }
}

// ---------------------------------------------------------
// HELPER FUNCTIONS
// ---------------------------------------------------------

function findShopifyOrderId(orderNumber) {
  try {
    var url1 = "orders.json?name=" + encodeURIComponent(orderNumber) + "&status=any&fields=id,order_number,name";
    var res1 = JSON.parse(fetchShopifySafe(url1, "get").getContentText());
    if (res1.orders) { for (var i = 0; i < res1.orders.length; i++) { if (String(res1.orders[i].order_number) === String(orderNumber)) return res1.orders[i].id; } }
    
    var url2 = "orders.json?name=" + encodeURIComponent("#" + orderNumber) + "&status=any&fields=id,order_number,name";
    var res2 = JSON.parse(fetchShopifySafe(url2, "get").getContentText());
    if (res2.orders) { for (var j = 0; j < res2.orders.length; j++) { if (String(res2.orders[j].order_number) === String(orderNumber)) return res2.orders[j].id; } }
    return null;
  } catch(e) { return null; }
}

function handleCheckCustomer(e) { 
  var p=e.parameter;
  var s=SpreadsheetApp.openById(SHEET_ID).getSheetByName("Customers").getDataRange().getValues();
  for(var i=1;i<s.length;i++) if(String(s[i][1])==String(p.phone)) return sendJSON({found:true,name:s[i][0],address:s[i][2]});
  return sendJSON({found:false});
}

function handleSaveOrder(e) {
  var p=e.parameter; var ss=SpreadsheetApp.openById(SHEET_ID);
  var os=ss.getSheetByName("Orders");
  var od=os.getDataRange().getValues();
  for(var i=1;i<od.length;i++) if(String(od[i][1])==String(p.oID)) return sendJSON({status:"error",message:"Exists!"});
  var cs=ss.getSheetByName("Customers");
  if(p.updateCustomer==='true') {
     var cd=cs.getDataRange().getValues();
     for(var i=1;i<cd.length;i++) if(String(cd[i][1])==String(p.cNumber)) { cs.getRange(i+1,1).setValue(p.cName); cs.getRange(i+1,3).setValue(p.cAddress); break; }
  } else if(p.isNewCustomer==='true') cs.appendRow([p.cName,"'"+p.cNumber,p.cAddress]);
  os.appendRow([p.oDate,"'"+p.oID,p.cName,"'"+p.cNumber,p.cAddress,p.oAmount,"","Pending",""]);
  return sendJSON({status:"success"});
}

function handleGetOrderImages(e) { return sendJSON({status: "success", images: []}); }

// ---------------------------------------------------------
// ORIGINAL SAFE DATA READER (100% RELIABLE)
// ---------------------------------------------------------
function getSavedOrderDetailsMap() {
  try {
    var sheet = SpreadsheetApp.openById(SHEET_ID).getSheetByName("Orders");
    var data = sheet.getDataRange().getValues();
    var map = {};
    for (var i = 1; i < data.length; i++) {
      map[String(data[i][1])] = { 
         name: data[i][2], 
         phone: String(data[i][3]), 
         address: data[i][4], 
         amount: data[i][5], 
         note: data[i][6], 
         status: data[i][7], 
         invoice: data[i][8] 
      };
    }
    return map;
  } catch (e) { 
    return {};
  }
}

function updateSheetCell(orderId, colIndex, value) {
  var ss = SpreadsheetApp.openById(SHEET_ID); var sheet = ss.getSheetByName("Orders"); var data = sheet.getDataRange().getValues();
  for (var i = 1; i < data.length; i++) { if (String(data[i][1]) === String(orderId)) { sheet.getRange(i + 1, colIndex).setValue(value); break; } }
}

function handleGetFulfillmentOrders(e) {
  try {
    var ss = SpreadsheetApp.openById(SHEET_ID); var sheet = ss.getSheetByName("Orders");
    var data = sheet.getDataRange().getValues(); var list = [];
    for (var i = 1; i < data.length; i++) {
      var status = String(data[i][7]);
      var statusLower = status.toLowerCase();
      if (statusLower.indexOf("product entry") !== -1 && statusLower.indexOf("courier") === -1) {
        list.push({ row: i + 1, date: data[i][0], id: String(data[i][1]), name: data[i][2], number: data[i][3], address: data[i][4], amount: data[i][5], invoice: data[i][8], status: status });
      }
    }
    return sendJSON({status: "success", orders: list.reverse()});
  } catch (err) { return sendJSON({status: "error", message: err.toString()}); }
}

function handleFulfillOrder(e) {
  var p = e.parameter;
  try {
    var realOrderId = findShopifyOrderId(p.orderId); if (!realOrderId) return sendJSON({status: "error", message: "Not found"});
    var foUrl = "orders/" + realOrderId + "/fulfillment_orders.json";
    var foJson = JSON.parse(fetchShopifySafe(foUrl, "get").getContentText());
    var openFO = foJson.fulfillment_orders.find(function(fo) { return fo.status === 'open' || fo.status === 'in_progress'; });
    if (!openFO) { updateSheetCell(p.orderId, 8, "Courier"); return sendJSON({status: "success", message: "Already fulfilled."}); }
    
    var fulfillUrl = "fulfillments.json";
    var payload = { "fulfillment": { "line_items_by_fulfillment_order": [{ "fulfillment_order_id": openFO.id }], "tracking_info": { "number": p.trackingNum, "url": p.trackingUrl, "company": "Courier" } } };
    var fulfillJson = JSON.parse(fetchShopifySafe(fulfillUrl, "post", payload).getContentText());
    
    if (fulfillJson.fulfillment || (fulfillJson.errors && JSON.stringify(fulfillJson.errors).indexOf("already fulfilled") > -1)) {
      updateSheetCell(p.orderId, 8, "Courier");
      return sendJSON({status: "success", message: "Fulfilled!"});
    } else { return sendJSON({status: "error", message: "Shopify Error"}); }
  } catch(err) { return sendJSON({status: "error", message: err.toString()}); }
}

function handleMarkShopifyPaid(e) {
  var orderNumber = e.parameter.orderId;
  try {
    var realId = findShopifyOrderId(orderNumber); if(!realId) return sendJSON({status: "error", message: "Order not found"});
    var url = "orders/" + realId + ".json?fields=id,financial_status,total_price,current_total_price";
    var order = JSON.parse(fetchShopifySafe(url, "get").getContentText()).order;
    if (order.financial_status === 'paid') return sendJSON({status: "success", message: "Already Paid"});
    
    var amountToCapture = order.current_total_price ? order.current_total_price : order.total_price;
    var transUrl = "orders/" + realId + "/transactions.json";
    var payload = { "transaction": { "kind": "sale", "gateway": "manual", "status": "success", "amount": amountToCapture } };
    var transRes = fetchShopifySafe(transUrl, "post", payload);
    if (JSON.parse(transRes.getContentText()).transaction) return sendJSON({status: "success", message: "Marked Paid"});
    
    payload = { "transaction": { "kind": "capture", "gateway": "manual", "status": "success", "amount": amountToCapture } };
    var captureRes = fetchShopifySafe(transUrl, "post", payload);
    if (JSON.parse(captureRes.getContentText()).transaction) return sendJSON({status: "success", message: "Captured & Paid"});
    return sendJSON({status: "error", message: "Failed"});
  } catch (err) { return sendJSON({status: "error", message: err.toString()}); }
}

function handleCancelShopifyOrder(e) {
  var p = e.parameter;
  var realId = findShopifyOrderId(p.orderId);
  if (!realId) return sendJSON({status: "error", message: "Not found"});
  var url = "orders/" + realId + "/cancel.json";
  var json = JSON.parse(fetchShopifySafe(url, "post", { "email": true, "restock": false }).getContentText());
  if (json.order || (json.errors && JSON.stringify(json.errors).indexOf('prior') > -1)) {
     updateSheetCell(p.orderId, 8, "Void");
     if (p.note) updateSheetCell(p.orderId, 7, p.note);
     return sendJSON({status: "success", message: "Order Cancelled"});
  }
  return sendJSON({status: "error", message: "Error"});
}

function handleRestockItem(e) {
  var p = e.parameter; var variantId = p.variantId; var quantity = parseInt(p.qty) || 1;
  var invItemId = getInventoryItemId(variantId); if (!invItemId) return sendJSON({status: "error", message: "Inv Item Not Found"});
  var url = "inventory_levels/adjust.json";
  var payload = { "inventory_item_id": invItemId, "location_id": LOCATION_ID, "available_adjustment": quantity };
  
  if (JSON.parse(fetchShopifySafe(url, "post", payload).getContentText()).inventory_level) return sendJSON({status: "success", message: "Restocked!"});
  return sendJSON({status: "error", message: "Failed"});
}

function getInventoryItemId(variantId) {
  try {
    var url = "variants/" + variantId + ".json";
    return JSON.parse(fetchShopifySafe(url, "get").getContentText()).variant.inventory_item_id;
  } catch(e) { return null; }
}

function handleUpdateSheetOrder(e) {
  var p = e.parameter; var ss = SpreadsheetApp.openById(SHEET_ID);
  var sheet = ss.getSheetByName("Orders"); var data = sheet.getDataRange().getValues();
  var rowIndex = -1;
  for (var i = 1; i < data.length; i++) { if (String(data[i][1]) == String(p.oID)) { rowIndex = i + 1; break; } }
  if (rowIndex === -1) return sendJSON({status: "error", message: "ID not found"});
  
  if (p.type === "amount") sheet.getRange(rowIndex, 6).setValue(p.value);
  else if (p.type === "note") sheet.getRange(rowIndex, 7).setValue(p.value);
  else if (p.type === "status") { 
    sheet.getRange(rowIndex, 8).setValue(p.value); 
    if (p.invoice === "DELETE") sheet.getRange(rowIndex, 9).clearContent();
    else if (p.invoice) sheet.getRange(rowIndex, 9).setValue("'" + p.invoice); 
  } 
  else if (p.type === "invoice") { 
    if (p.value === "DELETE") sheet.getRange(rowIndex, 9).clearContent();
    else sheet.getRange(rowIndex, 9).setValue("'" + p.value); 
  }
  return sendJSON({status: "success"});
}

function handleUpdateCustomerOnly(e) {
  var p = e.parameter;
  var ss = SpreadsheetApp.openById(SHEET_ID); var cs = ss.getSheetByName("Customers"); var data = cs.getDataRange().getValues();
  for (var i = 1; i < data.length; i++) { if (String(data[i][1]) == String(p.cNumber)) { cs.getRange(i+1, 1).setValue(p.cName); cs.getRange(i+1, 3).setValue(p.cAddress); return sendJSON({status:"success"}); } }
  return sendJSON({status:"error"});
}

function handleUpdateOrderCustomer(e) {
  var p = e.parameter; var ss = SpreadsheetApp.openById(SHEET_ID);
  var sheet = ss.getSheetByName("Orders"); var data = sheet.getDataRange().getValues();
  for (var i = 1; i < data.length; i++) { if (String(data[i][1]) == String(p.oID)) { var r = i + 1; sheet.getRange(r, 3).setValue(p.name); sheet.getRange(r, 4).setValue("'" + p.phone); sheet.getRange(r, 5).setValue(p.address); return sendJSON({status:"success"}); } }
  return sendJSON({status:"error"});
}

// ---------------------------------------------------------
// ASYNC PARALLEL IMAGE FETCHING (SUPER FAST)
// ---------------------------------------------------------
function fetchImagesForOrders(orders) {
  try {
    var productIds = [];
    orders.forEach(function(o) { if(o.line_items) o.line_items.forEach(function(i) { if(i.product_id) productIds.push(i.product_id); }); });
    var uniqueIds = [...new Set(productIds)];
    var map = {};

    if (uniqueIds.length > 0) {
      var requests = [];
      for (var i = 0; i < uniqueIds.length; i += 50) {
        var chunk = uniqueIds.slice(i, i + 50).join(",");
        var endpoint = "https://" + SHOP_DOMAIN + "/admin/api/" + API_VERSION + "/products.json?ids=" + chunk + "&fields=id,images,product_type";
        requests.push({
          url: endpoint,
          method: "get",
          headers: { 
            "X-Shopify-Access-Token": ACCESS_TOKEN,
            "Content-Type": "application/json"
          },
          muteHttpExceptions: true
        });
      }

      var responses = UrlFetchApp.fetchAll(requests);

      responses.forEach(function(res) {
         if (res.getResponseCode() === 200) {
            var json = JSON.parse(res.getContentText());
            json.products.forEach(function(p){ 
               map[p.id] = { src: (p.images.length>0 ? p.images[0].src : ""), type: p.product_type }; 
            });
         }
      });
    }
    return map;
  } catch(e) { 
    return {}; 
  }
}

function fetchImagesByOrderIds(orderIds) {
  try {
    var bulkUrl = "orders.json?status=any&limit=50&ids=" + orderIds.join(",") + "&fields=id,order_number,line_items";
    var ordJson = JSON.parse(fetchShopifySafe(bulkUrl, "get").getContentText());
    if (!ordJson.orders) return {};
    
    var productIds = []; var map = {};
    ordJson.orders.forEach(function(o) {
      var oNum = String(o.order_number);
      if (o.line_items) {
        o.line_items.forEach(function(item) { 
            if (item.product_id) { 
                productIds.push(item.product_id); 
                if(!map[oNum]) map[oNum]=[]; 
                map[oNum].push({pid:item.product_id, variant:item.variant_title, price:item.price, title:item.title, sku:item.sku}); 
            } 
        });
      }
    });
    
    var uniqueProdIds = [...new Set(productIds)]; var pImgs = {};
    if (uniqueProdIds.length > 0) {
       var requests = [];
       for (var i = 0; i < uniqueProdIds.length; i += 50) {
          var chunk = uniqueProdIds.slice(i, i + 50).join(",");
          var prodUrl = "https://" + SHOP_DOMAIN + "/admin/api/" + API_VERSION + "/products.json?ids=" + chunk + "&fields=id,images,product_type";
          requests.push({ url: prodUrl, method: "get", headers: { "X-Shopify-Access-Token": ACCESS_TOKEN, "Content-Type": "application/json" }, muteHttpExceptions: true });
       }
       
       var responses = UrlFetchApp.fetchAll(requests);
       responses.forEach(function(res) {
          if (res.getResponseCode() === 200) {
             JSON.parse(res.getContentText()).products.forEach(function(p) { pImgs[p.id] = { src: (p.images.length>0?p.images[0].src:""), type: p.product_type }; });
          }
       });
    }
    
    var finalMap = {};
    for (var oNum in map) {
       var items = map[oNum]; var finalList = [];
       items.forEach(function(item) { if(pImgs[item.pid]) finalList.push({ src: pImgs[item.pid].src, type: pImgs[item.pid].type, variant: item.variant, title: item.title, price: item.price, sku: item.sku, pid: item.pid }); });
       if (finalList.length > 0) finalMap[oNum] = finalList;
    }
    return finalMap;
  } catch (e) { return {}; }
}

function sendJSON(d) { return ContentService.createTextOutput(JSON.stringify(d)).setMimeType(ContentService.MimeType.JSON); }
