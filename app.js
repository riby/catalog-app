var express = require('express');
var bodyParser = require('body-parser');
var cassandra = require('cassandra-driver');
var async = require('async');
var path = require('path');

var Long = require('cassandra-driver').types.Long;

var cfenv = require("cfenv");
var appEnv = cfenv.getAppEnv();
var cassandra_ob=appEnv.getService("cassandraOrder");


var authProvider=new cassandra.auth.PlainTextAuthProvider(cassandra_ob.credentials["username"],cassandra_ob.credentials["password"] );

var keyspace_name=cassandra_ob.credentials["keyspace_name"];
var app = express();


/*var insertOrderInfo='INSERT INTO '+keyspace_name+'.order_info (order_id ,order_status ,order_total ,order_date ,estimated_delivery ,make , model , description , sku , quantity , price_today , carrier , shipping_address1 , shipping_address_city , shipping_address_state , shipping_address_postal_code , billing_address1 , billing_address_city , billing_address_state , billing_address_postal_code )'
+'values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);';
 title text,status text, stock_status text,stock_level text,description text
var updateOrderInfo='update '+keyspace_name+'.order_info set shipping_address1=?, shipping_address_city=?, shipping_address_state=?, shipping_address_postal_code=? WHERE order_id=?;';
*/
// view engine setup
var getItemBySku = 'SELECT * FROM '+keyspace_name+'.catalog WHERE sku = ?;';
var insertItem='INSERT INTO '+keyspace_name+'.catalog (sku ,title ,status ,stock_status ,stock_level ,description )'
+'values (?,?,?,?,?,?);';

app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'ejs');



app.use(bodyParser.json());

app.use(bodyParser.urlencoded({ extended: false }));




var client = new cassandra.Client( { authProvider:authProvider,contactPoints : cassandra_ob.credentials["node_ips"]} );
client.connect(function(err, result) {
    console.log('Connected.');
});


app.get('/kill', function(req, res) {
    process.exit()
});

app.get('/metadata', function(req, res) {
    res.send(client.hosts.slice(0).map(function (node) {
        return { address : node.address, rack : node.rack, datacenter : node.datacenter }
    }));
});



var port = process.env.VCAP_APP_PORT || 3000;


/**** Create Table for DB****/
 function createTable(){
    async.series([
        function(next) {
            client.execute('CREATE TABLE IF NOT EXISTS '+keyspace_name+'.catalog (sku bigint, title text,status text, stock_status text,stock_level text,description text, PRIMARY KEY(sku));',
                next);
        }],  function(err,result)
        {
            console.log(err);
         //   if(result!=null)
           // console.log(result);
        });
}


app.post('/delete', function(req, res) {
    async.series([
        function(next) {   
                client.execute('drop table '+keyspace_name+'.catalog;',next)

        }],   function(err,result)
        {
            console.log(err);
            if(result!=null)
            console.log(result);
        });
});




app.post('/additem', function(req, res) {
  
    client.execute(insertItem,
        [Long.fromString(req.body.sku),req.body.title,req.body.status,req.body.stock_status,req.body.stock_level,req.body.description],
        function(err,result)
        {
            console.log(err);
           // if(result!=null)
            //console.log(result);
        });
        res.send("SKU Inserted "+req.body.sku);
});



 app.get('/getitem/:id', function(req, res) {
    client.execute(getItemBySku, [ Long.fromString(req.params.id) ], function(err, result) {
        if (err) {
            res.status(404).send({ msg : 'SKU not found.' });
        } else {
        res.send(result.rows.map(function (node) {
       
        return { sku:node.sku,  title: node.title, status: node.status,  stock_status: node.stock_status,stock_level : node.stock_level,
          description:node.description

        }
    
    }));
              
             }
    });
});

app.get('/getallitems', function(req, res) {
  var getAllItems = 'SELECT * FROM '+keyspace_name+'.catalog';
    client.execute(getAllItems, function(err, result) {
        if (err) {
            res.status(404).send({ msg : 'Items not found.' });
        } else {
        res.send(result.rows.map(function (node) {
       
        return { sku:node.sku,  title: node.title, status: node.status,  stock_status: node.stock_status,stock_level : node.stock_level,
          description:node.description

        }
    
    }));
              
             }
    });
});




var updateOrderStatus='update '+keyspace_name+'.catalog set status=?,stock_status=?,stock_level=? WHERE sku=? ;';
 
 app.get('/update', function(req, res) {
    
    client.execute(updateOrderStatus,
        [req.body.status,req.body.stock_status,req.body.stock_level, Long.fromString(req.body.order_id)],
        function(err,result)
        {
            console.log(err);
          //  if(result!=null)
           // console.log(result);
        });
    res.send("SKU status changed to "+req.body.stock_status);
      
});


var server = app.listen( appEnv.port || 3000, function() {
    createTable();
    console.log('Listening on port %d', server.address().port);
});
