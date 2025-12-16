// ===========================================
// CONFIGURATION
// ===========================================
var SHOP_DOMAIN = "pff-premium-store.myshopify.com";
var ACCESS_TOKEN = "nnn"; // EKHANE APNAR TOKEN BOSHAN
var SHEET_ID = "1ScwvIVZLisTSvrFxmUj51TSJ2XERz8mxfKP6jvY7XQw";

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
  // --- MARK AS PAID ACTION ---
  if (action === "markShopifyPaid") return handleMarkShopifyPaid(e);
  // --- CANCEL/VOID ACTION ---
  if (action === "cancelShopifyOrder") return handleCancelShopifyOrder(e);

  return sendJSON({status: "error", message: "Invalid Action"});
}

// ---------------------------------------------------------
// 1. GET ORDERS (UPDATED SEARCH & DATE)
// ---------------------------------------------------------
function handleGetOrders(e) {
  try {
    var savedDataMap = getSavedOrderDetailsMap(); 
    var savedIds = Object.keys(savedDataMap);

    var limit = 50; 
    var params = e.parameter;
    var url = "https://" + SHOP_DOMAIN + "/admin/api/2024-01/orders.json?limit=" + limit;
    
    var userStatus = params.status || "any";
    if (["paid", "pending", "voided"].indexOf(userStatus) !== -1) {
      url += "&status=any&financial_status=" + userStatus;
    } else {
      url += "&status=" + userStatus;
    }

    if (params.name) {
      var q = params.name.trim();
      if (/^\d+$/.test(q) && q.length < 10) {
        q = "#" + q;
      }
      url += "&status=any&query=" + encodeURIComponent(q); 
    }

    if (params.created_at_min) url += "&created_at_min=" + params.created_at_min + "T00:00:00";
    if (params.created_at_max) url += "&created_at_max=" + params.created_at_max + "T23:59:59";
    
    if (params.page_info) {
        url = "https://" + SHOP_DOMAIN + "/admin/api/2024-01/orders.json?limit=" + limit + "&page_info=" + params.page_info;
    }

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

    return sendJSON({
      status: "success",
      orders: orders,
      images: imagesMap,
      savedIds: savedIds,       
      savedDetails: savedDataMap, 
      nextPage: nextCursor,
      prevPage: prevCursor
    });
  } catch (err) {
    return sendJSON({status: "error", message: err.toString()});
  }
}

// ---------------------------------------------------------
// 2. SHEET ORDERS LOGIC (WITH AUTO SYNC)
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
        id: String(r[1]),
        name: r[2],
        number: r[3],
        address: r[4],
        amount: r[5],
        note: r[6],
        status: r[7],
        invoice: r[8]
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

    // --- AUTO SYNC LOGIC ---
    // Check Shopify for these orders. If they are cancelled in Shopify but not in Sheet, update Sheet.
    syncCancelledOrders(pagedList, sheet);
    // -----------------------

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
// NEW: SYNC & CANCEL FUNCTIONS
// ---------------------------------------------------------
function syncCancelledOrders(orderList, sheet) {
  try {
     // Fetch latest 250 cancelled orders from Shopify to check against the current page
     var options = { "method": "get", "headers": { "X-Shopify-Access-Token": ACCESS_TOKEN }, "muteHttpExceptions":true };
     var url = "https://" + SHOP_DOMAIN + "/admin/api/2024-01/orders.json?status=cancelled&limit=250&fields=order_number,cancelled_at";
     var res = UrlFetchApp.fetch(url, options);
     var json = JSON.parse(res.getContentText());
     if (!json.orders) return;
     
     var cancelledMap = {};
     json.orders.forEach(function(o) { cancelledMap[String(o.order_number)] = true; });

     orderList.forEach(function(o) {
        // If Shopify says cancelled, but Sheet says NOT Void
        if (cancelledMap[o.id] && o.status !== "Void") {
            sheet.getRange(o.row, 8).setValue("Void"); // Update Sheet Status
            o.status = "Void"; // Update Response Object so UI shows it immediately
        }
     });
  } catch(e) {
    // Ignore sync errors to prevent page load failure
  }
}

function handleCancelShopifyOrder(e) {
  var p = e.parameter;
  var orderNumber = p.orderId;
  var note = p.note || "";

  // 1. Find Real Shopify ID from Order Number
  var realId = findShopifyOrderId(orderNumber);
  if (!realId) return sendJSON({status: "error", message: "Order #" + orderNumber + " not found in Shopify"});

  // 2. Call Cancel API
  try {
    var url = "https://" + SHOP_DOMAIN + "/admin/api/2024-01/orders/" + realId + "/cancel.json";
    // IMPORTANT: restock: false (Inventory will NOT increase)
    var payload = { "email": false, "restock": false }; 
    var options = {
      "method": "post",
      "headers": { "X-Shopify-Access-Token": ACCESS_TOKEN, "Content-Type": "application/json" },
      "payload": JSON.stringify(payload),
      "muteHttpExceptions": true
    };
    var res = UrlFetchApp.fetch(url, options);
    var json = JSON.parse(res.getContentText());

    // 3. Update Sheet if successful or already cancelled
    if (json.order || (json.errors && JSON.stringify(json.errors).indexOf('prior to this action') > -1)) {
       // Update Status to Void
       updateSheetCell(orderNumber, 8, "Void");
       // Update Note if provided
       if (note) updateSheetCell(orderNumber, 7, note);
       
       return sendJSON({status: "success", message: "Order Cancelled & Voided"});
    } else {
       return sendJSON({status: "error", message: "Shopify Error: " + JSON.stringify(json.errors || json)});
    }
  } catch(err) { return sendJSON({status: "error", message: err.toString()}); }
}

function findShopifyOrderId(orderNumber) {
  try {
    var options = { "method": "get", "headers": { "X-Shopify-Access-Token": ACCESS_TOKEN }, "muteHttpExceptions": true };
    // Try searching by name (usually #1234)
    var url = "https://" + SHOP_DOMAIN + "/admin/api/2024-01/orders.json?name=" + orderNumber + "&status=any&fields=id,order_number";
    var res = UrlFetchApp.fetch(url, options);
    var json = JSON.parse(res.getContentText());
    if (json.orders && json.orders.length > 0) {
      var match = json.orders.find(function(o) { return String(o.order_number) === String(orderNumber); });
      if (match) return match.id;
    }
    // Try with # prefix if needed
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

// ---------------------------------------------------------
// 3. IMAGE FETCHING FUNCTIONS
// ---------------------------------------------------------
function fetchImagesForOrders(orders) {
  try {
    var options = { "method": "get", "headers": { "X-Shopify-Access-Token": ACCESS_TOKEN, "Content-Type": "application/json" }, "muteHttpExceptions": true };
    var productIds = [];
    orders.forEach(function(o) { if(o.line_items) o.line_items.forEach(function(i) { if(i.product_id) productIds.push(i.product_id); }); });
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
  } catch(e) { return {};
  }
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
              orderNumberToItemsMap[oNum].push({ pid: item.product_id, variant: item.variant_title, 
              quantity: item.quantity });
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
        items.forEach(function(item) { if (productImages[item.pid]) finalItems.push({ src: productImages[item.pid], variant: item.variant, quantity: item.quantity }); });
        if (finalItems.length > 0) imagesMap[oNum] = finalItems;
      }
    }
    return imagesMap;
  } catch (e) { return {}; }
}

// ---------------------------------------------------------
// 4. FULFILLMENT & PAID ACTIONS
// ---------------------------------------------------------
function handleGetFulfillmentOrders(e) {
  try {
    var ss = SpreadsheetApp.openById(SHEET_ID);
    var sheet = ss.getSheetByName("Orders");
    var data = sheet.getDataRange().getValues();
    var list = [];
    for (var i = 1; i < data.length; i++) {
      var status = String(data[i][7]).toLowerCase();
      if (status.indexOf("product entry") !== -1 && status.indexOf("fulfilled") === -1) {
        list.push({ 
            row: i + 1, 
            date: data[i][0], 
            id: String(data[i][1]), 
            name: data[i][2], 
            number: data[i][3], // ADDED: Phone Number
            address: data[i][4], // ADDED: Address
            amount: data[i][5], // ADDED: Amount
            invoice: data[i][8] 
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
    var searchUrl = "https://" + SHOP_DOMAIN + "/admin/api/2024-01/orders.json?name=" + p.orderId + "&status=any&fields=id,order_number";
    var searchRes = UrlFetchApp.fetch(searchUrl, options);
    var searchJson = JSON.parse(searchRes.getContentText());
    var realOrderId = null;
    if (searchJson.orders && searchJson.orders.length > 0) {
      var matched = searchJson.orders.find(function(o) { return String(o.order_number) === String(p.orderId); });
      if (matched) realOrderId = matched.id;
    }
    if (!realOrderId) {
        var searchUrl2 = "https://" + SHOP_DOMAIN + "/admin/api/2024-01/orders.json?name=%23" + p.orderId + "&status=any&fields=id,order_number";
        var res2 = UrlFetchApp.fetch(searchUrl2, options);
        var json2 = JSON.parse(res2.getContentText());
        if (json2.orders && json2.orders.length > 0) {
            var matched = json2.orders.find(function(o) { return String(o.order_number) === String(p.orderId); });
            if (matched) realOrderId = matched.id;
        }
    }

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
  } catch(err) { return sendJSON({status: "error", message: err.toString()});
  }
}

function handleMarkShopifyPaid(e) {
  var orderNumber = e.parameter.orderId;
  try {
    var options = { "method": "get", "headers": { "X-Shopify-Access-Token": ACCESS_TOKEN, "Content-Type": "application/json" }, "muteHttpExceptions": true };
    var searchUrl = "https://" + SHOP_DOMAIN + "/admin/api/2024-01/orders.json?name=" + orderNumber + "&status=any&fields=id,order_number,financial_status,transactions";
    var searchRes = UrlFetchApp.fetch(searchUrl, options);
    var searchJson = JSON.parse(searchRes.getContentText());
    
    var order = null;
    if (searchJson.orders && searchJson.orders.length > 0) {
      order = searchJson.orders.find(function(o) { return String(o.order_number) === String(orderNumber); });
    }

    if (!order) {
        var searchUrl2 = "https://" + SHOP_DOMAIN + "/admin/api/2024-01/orders.json?name=%23" + orderNumber + "&status=any&fields=id,order_number,financial_status,transactions";
        var res2 = UrlFetchApp.fetch(searchUrl2, options);
        var json2 = JSON.parse(res2.getContentText());
        if (json2.orders && json2.orders.length > 0) {
            order = json2.orders.find(function(o) { return String(o.order_number) === String(orderNumber); });
        }
    }

    if (!order) return sendJSON({status: "error", message: "Order #" + orderNumber + " not found"});
    if (order.financial_status === 'paid') return sendJSON({status: "success", message: "Already Paid"});
    
    var transUrl = "https://" + SHOP_DOMAIN + "/admin/api/2024-01/orders/" + order.id + "/transactions.json";
    var payload = { "transaction": { "kind": "sale", "gateway": "manual", "status": "success", "amount": order.total_price } };
    var postOptions = {
      "method": "post",
      "headers": { "X-Shopify-Access-Token": ACCESS_TOKEN, "Content-Type": "application/json" },
      "payload": JSON.stringify(payload),
      "muteHttpExceptions": true
    };
    var transRes = UrlFetchApp.fetch(transUrl, postOptions);
    var transJson = JSON.parse(transRes.getContentText());

    if (transRes.getResponseCode() === 403 || transRes.getResponseCode() === 401) {
       return sendJSON({status: "error", message: "Permission Error. Check Access Scope"});
    }

    if (transJson.transaction) {
      return sendJSON({status: "success", message: "Marked Paid (Transaction)"});
    }

    var payloadCapture = { "transaction": { "kind": "capture", "status": "success" } };
    var postOptionsCapture = {
      "method": "post",
      "headers": { "X-Shopify-Access-Token": ACCESS_TOKEN, "Content-Type": "application/json" },
      "payload": JSON.stringify(payloadCapture),
      "muteHttpExceptions": true
    };
    var captureRes = UrlFetchApp.fetch(transUrl, postOptionsCapture);
    var captureJson = JSON.parse(captureRes.getContentText());

    if (captureJson.transaction) {
        return sendJSON({status: "success", message: "Marked Paid (Capture)"});
    }
    return sendJSON({status: "error", message: "Failed: " + JSON.stringify(transJson.errors || transJson)});
  } catch (err) {
    return sendJSON({status: "error", message: "System Error: " + err.toString()});
  }
}

function updateSheetToCourier(orderNumber) {
  var ss = SpreadsheetApp.openById(SHEET_ID);
  var sheet = ss.getSheetByName("Orders");
  var data = sheet.getDataRange().getValues();
  for (var i = 1; i < data.length; i++) {
    if (String(data[i][1]) === String(orderNumber)) {
       sheet.getRange(i + 1, 8).setValue("Courier");
       break;
    }
  }
}

// --- STANDARD HELPERS ---
function handleUpdateSheetOrder(e) {
  try {
    var p = e.parameter;
    var ss = SpreadsheetApp.openById(SHEET_ID);
    var sheet = ss.getSheetByName("Orders");
    var data = sheet.getDataRange().getValues();
    var rowIndex = -1;
    for (var i = 1; i < data.length; i++) { if (String(data[i][1]) == String(p.oID)) { rowIndex = i + 1;
    break; } }
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

function handleGetOrderImages(e) { return sendJSON({status: "success", images: []}); }

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
     for(var i=1;i<cd.length;i++) if(String(cd[i][1])==String(p.cNumber)) { cs.getRange(i+1,1).setValue(p.cName); cs.getRange(i+1,3).setValue(p.cAddress); break;
     }
  } else if(p.isNewCustomer==='true') cs.appendRow([p.cName,"'"+p.cNumber,p.cAddress]);
  os.appendRow([p.oDate,"'"+p.oID,p.cName,"'"+p.cNumber,p.cAddress,p.oAmount,"","Pending",""]);
  return sendJSON({status:"success"});
}

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
        address: data[i][4] 
      };
    }
    return map;
  } catch (e) {
    return {};
  }
}

function sendJSON(d) { return ContentService.createTextOutput(JSON.stringify(d)).setMimeType(ContentService.MimeType.JSON); }