var SHOP_DOMAIN = "pff-premium-store.myshopify.com";
var ACCESS_TOKEN = "xxxxxxxxxx"; // YOUR TOKEN
var SHEET_ID = "xxxxxxxxxxx";

function doGet(e) {
  var action = e.parameter.action;
  
  if (action === "getOrders") return handleGetOrders(e);
  if (action === "checkCustomer") return handleCheckCustomer(e);
  if (action === "saveOrder") return handleSaveOrder(e);
  if (action === "getSheetOrders") return handleGetSheetOrders(e);
  if (action === "updateSheetOrder") return handleUpdateSheetOrder(e);
  if (action === "getOrderImages") return handleGetOrderImages(e);
  if (action === "updateCustomerOnly") return handleUpdateCustomerOnly(e);

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

  return sendJSON({status: "error", message: "Invalid Action"});
}

// ---------------------------------------------------------
// 1. GET ORDERS (Shopify + Saved Data - ENHANCED SEARCH)
// ---------------------------------------------------------
function handleGetOrders(e) {
  try {
    var savedDataMap = getSavedOrderDetailsMap();
    var savedIds = Object.keys(savedDataMap);
    var limit = 50; 
    var params = e.parameter;
    var url = "https://" + SHOP_DOMAIN + "/admin/api/2024-01/orders.json?limit=" + limit;
    
    var userStatus = params.status || "any";
    if (["paid", "pending", "voided", "authorized", "partially_paid", "refunded"].indexOf(userStatus) !== -1) {
      url += "&status=any&financial_status=" + userStatus;
    } else {
      url += "&status=" + userStatus;
    }

    if (params.name) {
      var q = params.name.trim();
      var qLower = q.toLowerCase();
      var searchTerms = [];
      for (var id in savedDataMap) {
        var s = savedDataMap[id];
        var sName = String(s.name || "").toLowerCase();
        var sPhone = String(s.phone || "").replace(/[^0-9]/g, ""); 
        var qClean = qLower.replace(/[^0-9]/g, "");
        var sInvoice = String(s.invoice || "").toLowerCase();

        var isMatch = false;
        if (sName.indexOf(qLower) > -1) isMatch = true;
        else if (qClean.length > 5 && sPhone.indexOf(qClean) > -1) isMatch = true;
        else if (sInvoice.indexOf(qLower) > -1) isMatch = true;

        if (isMatch) searchTerms.push("name:" + id); 
      }

      var rawQuery = q;
      if (/^\d+$/.test(rawQuery) && rawQuery.length < 10) rawQuery = "#" + rawQuery; 
      searchTerms.push(rawQuery);

      var uniqueTerms = [...new Set(searchTerms)];
      if (uniqueTerms.length > 25) uniqueTerms = uniqueTerms.slice(0, 25); 
      url += "&status=any&query=" + encodeURIComponent(uniqueTerms.join(" OR "));
    }

    if (params.created_at_min) url += "&created_at_min=" + params.created_at_min + "T00:00:00";
    if (params.created_at_max) url += "&created_at_max=" + params.created_at_max + "T23:59:59";
    if (params.page_info) url = "https://" + SHOP_DOMAIN + "/admin/api/2024-01/orders.json?limit=" + limit + "&page_info=" + params.page_info;

    var options = { "method": "get", "headers": { "X-Shopify-Access-Token": ACCESS_TOKEN, "Content-Type": "application/json" }, "muteHttpExceptions": true };
    var response = UrlFetchApp.fetch(url, options);
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
    var imagesMap = fetchImagesForOrders(orders);

    var entryAmountTotal = 0;
    for (var id in savedDataMap) {
        if (savedDataMap[id].status && savedDataMap[id].status.toLowerCase().indexOf("product entry") !== -1) {
            var val = parseFloat(String(savedDataMap[id].amount).replace(/[^0-9.-]+/g,""));
            if (!isNaN(val)) entryAmountTotal += val;
        }
    }

    return sendJSON({
      status: "success",
      orders: orders,
      images: imagesMap,
      savedIds: savedIds,       
      savedDetails: savedDataMap, 
      entryAmount: entryAmountTotal,
      nextPage: nextCursor,
      prevPage: prevCursor
    });
  } catch (err) {
    return sendJSON({status: "error", message: err.toString()});
  }
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
        row: i + 2,
        date: r[0],
        id: String(r[1]).replace(/'/g, ""), 
        name: r[2],
        number: String(r[3]), 
        address: r[4],
        amount: r[5],
        note: r[6],
        status: r[7],
        invoice: r[8]
      });
    }

    if (e.parameter.search) {
      var q = e.parameter.search.toLowerCase().trim();
      orderList = orderList.filter(function(o) {
        var id = String(o.id).toLowerCase();
        var name = String(o.name).toLowerCase();
        var phone = String(o.number).replace(/['\s-]/g, "").toLowerCase();
        return id.indexOf(q) > -1 || name.indexOf(q) > -1 || phone.indexOf(q) > -1;
      });
    }

    orderList.reverse();

    if (e.parameter.dateFrom && e.parameter.dateTo) {
      var fromDate = new Date(e.parameter.dateFrom);
      fromDate.setHours(0,0,0,0);
      var toDate = new Date(e.parameter.dateTo); toDate.setHours(23,59,59,999);
      orderList = orderList.filter(function(o) {
        var d = o.date; if (typeof d === 'string') d = new Date(d);
        return d >= fromDate && d <= toDate;
      });
    }

    if (e.parameter.export === 'true') {
      return sendJSON({status: "success", orders: orderList});
    }

    var page = parseInt(e.parameter.page) || 1;
    var limit = 50;
    var offset = (page - 1) * limit;
    var pagedList = orderList.slice(offset, offset + limit);
    var hasMore = (offset + limit) < orderList.length;

    syncCancelledOrders(pagedList, sheet);

    var orderIdsForImages = pagedList.map(function(o){ return o.id; });
    var imagesMap = {};
    if (orderIdsForImages.length > 0) {
      imagesMap = fetchImagesByOrderIds(orderIdsForImages);
    }
    
    return sendJSON({
      status: "success", 
      orders: pagedList, 
      images: imagesMap,
      hasMore: hasMore,
      total: orderList.length
    });
  } catch (err) {
    return sendJSON({status: "error", message: err.toString()});
  }
}

// ---------------------------------------------------------
// UPDATED: SEARCH PRODUCTS (GRAPHQL - Name, SKU, Type)
// ---------------------------------------------------------
function handleSearchProducts(e) {
  var q = e.parameter.q;
  if(!q) return sendJSON({status: "error", message: "Query missing"});
  
  try {
     // Prepare wildcard search term
     var term = "*" + q + "*";
     var searchString = "title:" + term + " OR sku:" + term + " OR product_type:" + term;

     var gql = `query search($q: String!) {
       products(first: 10, query: $q) {
         edges {
           node {
             id
             title
             images(first: 1) { edges { node { url } } }
             variants(first: 5) {
               edges {
                 node {
                   id
                   title
                   sku
                   price
                 }
               }
             }
           }
         }
       }
     }`;

     var json = shopifyGraphQL(gql, { q: searchString });
     var results = [];
     
     if(json.data && json.data.products) {
        json.data.products.edges.forEach(function(edge){
            var p = edge.node;
            var pId = p.id.split("/").pop(); // Strip GID prefix
            var img = (p.images.edges.length > 0) ? p.images.edges[0].node.url : "";
            
            p.variants.edges.forEach(function(vEdge){
               var v = vEdge.node;
               var vId = v.id.split("/").pop(); // Strip GID prefix
               results.push({
                   id: vId,
                   product_id: pId,
                   title: p.title,
                   variant_title: v.title === 'Default Title' ? '' : v.title,
                   sku: v.sku, // Pass SKU for display
                   price: v.price,
                   image: img
               });
            });
        });
     }
     return sendJSON({status: "success", results: results});
  } catch(e) { return sendJSON({status: "error", message: e.toString()}); }
}

// ---------------------------------------------------------
// NEW: EDIT SHOPIFY ORDER (GRAPHQL)
// ---------------------------------------------------------
function handleEditShopifyOrder(e) {
  var p = e.parameter;
  var orderId = p.orderId; // Standard ID (e.g., 56644...)
  var additions = p.additions ? JSON.parse(p.additions) : []; // [{variantId, qty}]
  var removals = p.removals ? JSON.parse(p.removals) : []; // [{lineItemId, quantity: 0}]

  try {
     // 1. Convert Order ID to GID
     var orderGid = "gid://shopify/Order/" + orderId;

     // 2. Begin Edit
     var beginQuery = `mutation beginEdit($id: ID!) { orderEditBegin(id: $id) { calculatedOrder { id lineItems(first:50) { edges { node { id variant { id } } } } } userErrors { field message } } }`;
     var beginRes = shopifyGraphQL(beginQuery, { id: orderGid });
     if(beginRes.data.orderEditBegin.userErrors.length > 0) {
        return sendJSON({status: "error", message: "Begin Edit Failed: " + JSON.stringify(beginRes.data.orderEditBegin.userErrors)});
     }
     var calcId = beginRes.data.orderEditBegin.calculatedOrder.id;
     var currentLines = beginRes.data.orderEditBegin.calculatedOrder.lineItems.edges;

     // 3. Process Removals (Set Quantity to 0)
     for(var i=0; i<removals.length; i++) {
        var rem = removals[i];
        // Find line in calculated order that matches this variant (rem.variantId is sent from front)
        var targetLine = currentLines.find(function(edge) { 
             return String(edge.node.variant.id).indexOf(String(rem.variantId)) > -1; // variant.id is GID usually
        });
        
        if(targetLine) {
             var remQuery = `mutation editQty($id: ID!, $lineItemId: ID!, $qty: Int!) { orderEditSetQuantity(id: $id, lineItemId: $lineItemId, quantity: $qty) { calculatedOrder { id } userErrors { field message } } }`;
             shopifyGraphQL(remQuery, { id: calcId, lineItemId: targetLine.node.id, qty: 0 });
        }
     }

     // 4. Process Additions
     for(var j=0; j<additions.length; j++) {
        var add = additions[j];
        var varGid = "gid://shopify/ProductVariant/" + add.variantId;
        var addQuery = `mutation addVar($id: ID!, $variantId: ID!, $qty: Int!) { orderEditAddVariant(id: $id, variantId: $variantId, quantity: $qty) { calculatedOrder { id } userErrors { field message } } }`;
        shopifyGraphQL(addQuery, { id: calcId, variantId: varGid, qty: parseInt(add.qty) });
     }

     // 5. Commit Edit
     var commitQuery = `mutation commitEdit($id: ID!) { orderEditCommit(id: $id) { order { id } userErrors { field message } } }`;
     var commitRes = shopifyGraphQL(commitQuery, { id: calcId });
     
     if(commitRes.data.orderEditCommit.userErrors.length > 0) {
        return sendJSON({status: "error", message: "Commit Failed: " + JSON.stringify(commitRes.data.orderEditCommit.userErrors)});
     }

     return sendJSON({status: "success", message: "Order Updated on Shopify"});
  } catch(err) {
     return sendJSON({status: "error", message: "Shopify Edit Error: " + err.toString()});
  }
}

function shopifyGraphQL(query, variables) {
  var url = "https://" + SHOP_DOMAIN + "/admin/api/2024-01/graphql.json";
  var options = {
    "method": "post",
    "headers": { "X-Shopify-Access-Token": ACCESS_TOKEN, "Content-Type": "application/json" },
    "payload": JSON.stringify({ query: query, variables: variables }),
    "muteHttpExceptions": true
  };
  var res = UrlFetchApp.fetch(url, options);
  return JSON.parse(res.getContentText());
}

// ---------------------------------------------------------
// EXISTING HELPER FUNCTIONS (Preserved)
// ---------------------------------------------------------
function handleCheckCustomer(e) { 
  var p=e.parameter; var s=SpreadsheetApp.openById(SHEET_ID).getSheetByName("Customers").getDataRange().getValues();
  for(var i=1;i<s.length;i++) if(String(s[i][1])==String(p.phone)) return sendJSON({found:true,name:s[i][0],address:s[i][2]});
  return sendJSON({found:false});
}

function handleSaveOrder(e) {
  var p=e.parameter; var ss=SpreadsheetApp.openById(SHEET_ID);
  var os=ss.getSheetByName("Orders"); var od=os.getDataRange().getValues();
  for(var i=1;i<od.length;i++) if(String(od[i][1])==String(p.oID)) return sendJSON({status:"error",message:"Exists!"});
  var cs=ss.getSheetByName("Customers");
  if(p.updateCustomer==='true') {
     var cd=cs.getDataRange().getValues();
     for(var i=1;i<cd.length;i++) if(String(cd[i][1])==String(p.cNumber)) { cs.getRange(i+1,1).setValue(p.cName);
         cs.getRange(i+1,3).setValue(p.cAddress); break;
    }
  } else if(p.isNewCustomer==='true') cs.appendRow([p.cName,"'"+p.cNumber,p.cAddress]);
  os.appendRow([p.oDate,"'"+p.oID,p.cName,"'"+p.cNumber,p.cAddress,p.oAmount,"","Pending",""]);
  return sendJSON({status:"success"});
}

function handleGetOrderImages(e) { return sendJSON({status: "success", images: []}); }

function getSavedOrderDetailsMap() {
  try {
    var sheet = SpreadsheetApp.openById(SHEET_ID).getSheetByName("Orders");
    var data = sheet.getDataRange().getValues();
    var map = {};
    for (var i = 1; i < data.length; i++) {
      var id = String(data[i][1]);
      map[id] = {
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
  } catch (e) { return {}; }
}

function findShopifyOrderId(orderNumber) {
  try {
    var options = { "method": "get", "headers": { "X-Shopify-Access-Token": ACCESS_TOKEN }, "muteHttpExceptions": true };
    var url = "https://" + SHOP_DOMAIN + "/admin/api/2024-01/orders.json?name=" + orderNumber + "&status=any&fields=id,order_number";
    var res = UrlFetchApp.fetch(url, options);
    var json = JSON.parse(res.getContentText());
    if (json.orders && json.orders.length > 0) {
      var match = json.orders.find(function(o) { return String(o.order_number) === String(orderNumber); });
      if (match) return match.id;
    }
    var url2 = "https://" + SHOP_DOMAIN + "/admin/api/2024-01/orders.json?name=%23" + orderNumber + "&status=any&fields=id,order_number";
    var res2 = UrlFetchApp.fetch(url2, options);
    var json2 = JSON.parse(res2.getContentText());
    if (json2.orders && json2.orders.length > 0) {
      var match = json2.orders.find(function(o) { return String(o.order_number) === String(orderNumber); });
      if (match) return match.id;
    }
    return null;
  } catch(e) { return null; }
}

function updateSheetToCourier(orderNumber) {
  updateSheetCell(orderNumber, 8, "Courier");
}

function updateSheetCell(orderId, colIndex, value) {
  var ss = SpreadsheetApp.openById(SHEET_ID);
  var sheet = ss.getSheetByName("Orders");
  var data = sheet.getDataRange().getValues();
  for (var i = 1; i < data.length; i++) {
    if (String(data[i][1]) === String(orderId)) {
      sheet.getRange(i + 1, colIndex).setValue(value);
      break;
    }
  }
}

function handleGetFulfillmentOrders(e) {
  try {
    var ss = SpreadsheetApp.openById(SHEET_ID);
    var sheet = ss.getSheetByName("Orders");
    var data = sheet.getDataRange().getValues();
    var list = [];
    for (var i = 1; i < data.length; i++) {
      var status = String(data[i][7]);
      var statusLower = status.toLowerCase();
      if (statusLower.indexOf("product entry") !== -1 && statusLower.indexOf("courier") === -1) {
        list.push({ 
            row: i + 1, 
            date: data[i][0], 
            id: String(data[i][1]), 
            name: data[i][2], 
            number: data[i][3],
            address: data[i][4],
            amount: data[i][5], 
            invoice: data[i][8],
            status: status 
        });
      }
    }
    return sendJSON({status: "success", orders: list.reverse()});
  } catch (err) { return sendJSON({status: "error", message: err.toString()}); }
}

function handleFulfillOrder(e) {
  var p = e.parameter;
  try {
    var options = { "method": "get", "headers": { "X-Shopify-Access-Token": ACCESS_TOKEN, "Content-Type": "application/json" }, "muteHttpExceptions": true };
    var realOrderId = findShopifyOrderId(p.orderId);
    if (!realOrderId) return sendJSON({status: "error", message: "Shopify Order #" + p.orderId + " not found."});
    
    var foUrl = "https://" + SHOP_DOMAIN + "/admin/api/2024-01/orders/" + realOrderId + "/fulfillment_orders.json";
    var foRes = UrlFetchApp.fetch(foUrl, options);
    var foJson = JSON.parse(foRes.getContentText());
    
    if (!foJson.fulfillment_orders || foJson.fulfillment_orders.length === 0) {
       updateSheetToCourier(p.orderId);
       return sendJSON({status: "success", message: "Already fulfilled on Shopify."});
    }
    var openFO = foJson.fulfillment_orders.find(function(fo) { return fo.status === 'open' || fo.status === 'in_progress'; });
    if (!openFO) {
       updateSheetToCourier(p.orderId);
       return sendJSON({status: "success", message: "Already fulfilled. Marked Courier."});
    }

    var fulfillUrl = "https://" + SHOP_DOMAIN + "/admin/api/2024-01/fulfillments.json";
    var payload = { "fulfillment": { "line_items_by_fulfillment_order": [{ "fulfillment_order_id": openFO.id }], "tracking_info": { "number": p.trackingNum, "url": p.trackingUrl, "company": "Courier" } } };
    var postOptions = { "method": "post", "headers": { "X-Shopify-Access-Token": ACCESS_TOKEN, "Content-Type": "application/json" }, "payload": JSON.stringify(payload), "muteHttpExceptions": true };
    var fulfillRes = UrlFetchApp.fetch(fulfillUrl, postOptions);
    var fulfillJson = JSON.parse(fulfillRes.getContentText());
    if (fulfillJson.fulfillment || (fulfillJson.errors && JSON.stringify(fulfillJson.errors).indexOf("already fulfilled") > -1)) {
      updateSheetToCourier(p.orderId);
      return sendJSON({status: "success", message: "Fulfilled & Updated!"});
    } else {
      return sendJSON({status: "error", message: "Shopify Error: " + JSON.stringify(fulfillJson.errors)});
    }
  } catch(err) { return sendJSON({status: "error", message: err.toString()}); }
}

function handleMarkShopifyPaid(e) {
  var orderNumber = e.parameter.orderId;
  try {
    var realId = findShopifyOrderId(orderNumber);
    if(!realId) return sendJSON({status: "error", message: "Order not found"});
    var options = { "method": "get", "headers": { "X-Shopify-Access-Token": ACCESS_TOKEN, "Content-Type": "application/json" }, "muteHttpExceptions": true };
    var url = "https://" + SHOP_DOMAIN + "/admin/api/2024-01/orders/" + realId + ".json?fields=id,financial_status,total_price,current_total_price";
    var res = UrlFetchApp.fetch(url, options);
    var order = JSON.parse(res.getContentText()).order;
    if (order.financial_status === 'paid') return sendJSON({status: "success", message: "Already Paid"});
    var amountToCapture = order.current_total_price ? order.current_total_price : order.total_price;
    var transUrl = "https://" + SHOP_DOMAIN + "/admin/api/2024-01/orders/" + order.id + "/transactions.json";
    var payload = { "transaction": { "kind": "sale", "gateway": "manual", "status": "success", "amount": amountToCapture } };
    var postOptions = { "method": "post", "headers": { "X-Shopify-Access-Token": ACCESS_TOKEN, "Content-Type": "application/json" }, "payload": JSON.stringify(payload), "muteHttpExceptions": true };
    var transRes = UrlFetchApp.fetch(transUrl, postOptions);
    var transJson = JSON.parse(transRes.getContentText());
    if (transJson.transaction) return sendJSON({status: "success", message: "Marked Paid"});
    return sendJSON({status: "error", message: "Failed: " + JSON.stringify(transJson.errors)});
  } catch (err) { return sendJSON({status: "error", message: err.toString()}); }
}

function handleCancelShopifyOrder(e) {
  var p = e.parameter;
  var orderNumber = p.orderId;
  var note = p.note || "";
  var realId = findShopifyOrderId(orderNumber);
  if (!realId) return sendJSON({status: "error", message: "Not found"});
  try {
    var url = "https://" + SHOP_DOMAIN + "/admin/api/2024-01/orders/" + realId + "/cancel.json";
    var payload = { "email": false, "restock": false };
    var options = { "method": "post", "headers": { "X-Shopify-Access-Token": ACCESS_TOKEN, "Content-Type": "application/json" }, "payload": JSON.stringify(payload), "muteHttpExceptions": true };
    var res = UrlFetchApp.fetch(url, options);
    var json = JSON.parse(res.getContentText());
    if (json.order || (json.errors && JSON.stringify(json.errors).indexOf('prior to this action') > -1)) {
       updateSheetCell(orderNumber, 8, "Void");
       if (note) updateSheetCell(orderNumber, 7, note);
       return sendJSON({status: "success", message: "Order Cancelled & Voided"});
    }
    return sendJSON({status: "error", message: "Error: " + JSON.stringify(json.errors)});
  } catch(err) { return sendJSON({status: "error", message: err.toString()}); }
}

function handleRestockItem(e) {
  var p = e.parameter;
  var variantId = p.variantId;
  var quantity = parseInt(p.qty) || 1;
  if (!variantId) return sendJSON({status: "error", message: "Variant ID missing"});
  try {
    var invItemId = getInventoryItemId(variantId);
    if (!invItemId) return sendJSON({status: "error", message: "Inv Item Not Found"});
    var locationId = getPrimaryLocationId();
    if (!locationId) return sendJSON({status: "error", message: "Location Not Found"});
    var url = "https://" + SHOP_DOMAIN + "/admin/api/2024-01/inventory_levels/adjust.json";
    var payload = { "inventory_item_id": invItemId, "location_id": locationId, "available_adjustment": quantity };
    var options = { "method": "post", "headers": { "X-Shopify-Access-Token": ACCESS_TOKEN, "Content-Type": "application/json" }, "payload": JSON.stringify(payload), "muteHttpExceptions": true };
    var res = UrlFetchApp.fetch(url, options);
    var json = JSON.parse(res.getContentText());
    if (json.inventory_level) return sendJSON({status: "success", message: "Restocked!"});
    else return sendJSON({status: "error", message: "Restock Failed: " + JSON.stringify(json)});
  } catch(err) { return sendJSON({status: "error", message: err.toString()}); }
}

function getInventoryItemId(variantId) {
  try {
    var url = "https://" + SHOP_DOMAIN + "/admin/api/2024-01/variants/" + variantId + ".json";
    var options = { "method": "get", "headers": { "X-Shopify-Access-Token": ACCESS_TOKEN }, "muteHttpExceptions": true };
    var res = UrlFetchApp.fetch(url, options);
    var json = JSON.parse(res.getContentText());
    return json.variant ? json.variant.inventory_item_id : null;
  } catch(e) { return null; }
}

function getPrimaryLocationId() {
  try {
    var url = "https://" + SHOP_DOMAIN + "/admin/api/2024-01/locations.json";
    var options = { "method": "get", "headers": { "X-Shopify-Access-Token": ACCESS_TOKEN }, "muteHttpExceptions": true };
    var res = UrlFetchApp.fetch(url, options);
    var json = JSON.parse(res.getContentText());
    if (json.locations && json.locations.length > 0) {
      var loc = json.locations.find(function(l) { return l.active; });
      return loc ? loc.id : json.locations[0].id;
    }
    return null;
  } catch(e) { return null; }
}

function handleUpdateSheetOrder(e) {
  try {
    var p = e.parameter;
    var ss = SpreadsheetApp.openById(SHEET_ID);
    var sheet = ss.getSheetByName("Orders");
    var data = sheet.getDataRange().getValues();
    var rowIndex = -1;
    for (var i = 1; i < data.length; i++) { if (String(data[i][1]) == String(p.oID)) { rowIndex = i + 1; break; } }
    if (rowIndex === -1) return sendJSON({status: "error", message: "ID not found"});
    if (p.type === "amount") sheet.getRange(rowIndex, 6).setValue(p.value);
    else if (p.type === "note") sheet.getRange(rowIndex, 7).setValue(p.value);
    else if (p.type === "status") {
      sheet.getRange(rowIndex, 8).setValue(p.value);
      if (p.invoice === "DELETE") sheet.getRange(rowIndex, 9).clearContent();
      else if (p.invoice) sheet.getRange(rowIndex, 9).setValue("'" + p.invoice);
    } else if (p.type === "invoice") {
       if (p.value === "DELETE") sheet.getRange(rowIndex, 9).clearContent();
       else sheet.getRange(rowIndex, 9).setValue("'" + p.value);
    }
    return sendJSON({status: "success"});
  } catch (err) { return sendJSON({status: "error", message: err.toString()}); }
}

function handleUpdateCustomerOnly(e) {
  try {
    var p = e.parameter;
    var ss = SpreadsheetApp.openById(SHEET_ID);
    var custSheet = ss.getSheetByName("Customers");
    var data = custSheet.getDataRange().getValues();
    var found = false;
    for (var i = 1; i < data.length; i++) {
      if (String(data[i][1]) == String(p.cNumber)) {
         custSheet.getRange(i+1, 1).setValue(p.cName);
         custSheet.getRange(i+1, 3).setValue(p.cAddress);
         found = true; break;
      }
    }
    return sendJSON({status: found ? "success" : "error", message: found ? "Updated!" : "Not found"});
  } catch (err) { return sendJSON({status: "error", message: err.toString()}); }
}

function fetchImagesForOrders(orders) {
  try {
    var options = { "method": "get", "headers": { "X-Shopify-Access-Token": ACCESS_TOKEN, "Content-Type": "application/json" }, "muteHttpExceptions": true };
    var productIds = [];
    orders.forEach(function(o) { 
        if(o.line_items) {
            o.line_items.forEach(function(i) { if(i.product_id) productIds.push(i.product_id); }); 
        }
    });
    var uniqueIds = [...new Set(productIds)];
    var map = {};
    if (uniqueIds.length > 0) {
      var BATCH_SIZE = 50;
      for (var i = 0; i < uniqueIds.length; i += BATCH_SIZE) {
        var chunk = uniqueIds.slice(i, i + BATCH_SIZE);
        var url = "https://" + SHOP_DOMAIN + "/admin/api/2024-01/products.json?ids=" + chunk.join(",") + "&fields=id,images";
        var res = UrlFetchApp.fetch(url, options);
        var json = JSON.parse(res.getContentText());
        if(json.products) json.products.forEach(function(p){ if(p.images && p.images.length > 0) map[p.id] = p.images[0].src; });
      }
    }
    return map;
  } catch(e) { return {}; }
}

function fetchImagesByOrderIds(orderIds) {
  try {
    var options = { "method": "get", "headers": { "X-Shopify-Access-Token": ACCESS_TOKEN, "Content-Type": "application/json" }, "muteHttpExceptions": true };
    var bulkUrl = "https://" + SHOP_DOMAIN + "/admin/api/2024-01/orders.json?status=any&limit=250&fields=id,order_number,line_items";
    var ordResp = UrlFetchApp.fetch(bulkUrl, options);
    var ordJson = JSON.parse(ordResp.getContentText());
    if (!ordJson.orders) return {};

    var productIds = [];
    var orderNumberToItemsMap = {};
    ordJson.orders.forEach(function(o) {
      var oNum = String(o.order_number);
      if (orderIds.indexOf(oNum) !== -1) {
        if(o.line_items) {
          if(!orderNumberToItemsMap[oNum]) orderNumberToItemsMap[oNum] = [];
          o.line_items.forEach(function(item) {
            if (item.product_id) {
              productIds.push(item.product_id);
              orderNumberToItemsMap[oNum].push({ 
                  pid: item.product_id, 
                  variant: item.variant_title,
                  variant_id: item.variant_id, 
                  quantity: item.quantity,
                  price: item.price, 
                  title: item.title, 
                  fulfillable_quantity: item.fulfillable_quantity,
                  fulfillment_status: item.fulfillment_status
               });
            }
          });
        }
      }
    });
    var uniqueProdIds = [...new Set(productIds)];
    var imagesMap = {}; 
    if (uniqueProdIds.length > 0) {
      var BATCH_SIZE = 50;
      var productImages = {};
      for (var i = 0; i < uniqueProdIds.length; i += BATCH_SIZE) {
        var chunk = uniqueProdIds.slice(i, i + BATCH_SIZE);
        var prodUrl = "https://" + SHOP_DOMAIN + "/admin/api/2024-01/products.json?ids=" + chunk.join(",") + "&fields=id,images";
        var prodResp = UrlFetchApp.fetch(prodUrl, options);
        var prodJson = JSON.parse(prodResp.getContentText());
        if (prodJson.products) prodJson.products.forEach(function(p) { if (p.images && p.images.length > 0) productImages[p.id] = p.images[0].src; });
      }
      for (var oNum in orderNumberToItemsMap) {
        var items = orderNumberToItemsMap[oNum];
        var finalItems = [];
        items.forEach(function(item) { 
            if (productImages[item.pid]) {
                finalItems.push({ 
                    src: productImages[item.pid], 
                    variant: item.variant,
                    variant_id: item.variant_id,
                    title: item.title,
                    price: item.price,
                    quantity: item.quantity,
                    fulfillable_quantity: item.fulfillable_quantity,
                    fulfillment_status: item.fulfillment_status
                }); 
            }
        });
        if (finalItems.length > 0) imagesMap[oNum] = finalItems;
      }
    }
    return imagesMap;
  } catch (e) { return {}; }
}

function sendJSON(d) { return ContentService.createTextOutput(JSON.stringify(d)).setMimeType(ContentService.MimeType.JSON); }
