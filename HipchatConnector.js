(function() {

    var getRows = function(url, dataProcessingFn, dataDoneCallback) {
        var fullUrl = url + "&auth_token=" + tableau.password;
        $.getJSON(fullUrl)
        .then(function(data) {
            var nextUrl = dataProcessingFn(data);
            if (!!nextUrl) {
                getRows(nextUrl, dataProcessingFn, dataDoneCallback);
            } else {
                dataDoneCallback();
            }
        }).fail(function(e, a, c) {
            console.log("Error occured");
            dataDoneCallback();
        });
    };


    var myConnector = tableau.makeConnector();
    
    myConnector.getSchema = function(schemaCallback) {
         var roomsCols = [
             { id: "name", dataType: "string" },
             { id: "privacy", dataType: "string" },
             { id: "roomId", dataType: "int" },
             { id: "link", dataType: "string" },
         ]
         
         var roomsTable = {
             alias: "Rooms",
             id: "rooms",
             columns: roomsCols
         };
         
         var userCols = [
            { id: "user_id", dataType: "string", alias: "User Id", "filterable" : true },
            { id: "name", dataType: "string", alias: "User Name" },
            { id: "mention_name", dataType: "string", alias: "User Mention (@) Name"},
            { id: "title", dataType: "string" },
            { id: "last_active", dataType: "datetime", alias: "Last Active Time" },
            { id: "created", dataType: "datetime", alias: "Created Date" },
            { id: "status", dataType: "string" },
            { id: "status_message", dataType: "string" },
            { id: "idle_seconds", dataType: "int" },
            { id: "is_online", dataType: "bool" },
            { id: "email", dataType: "string" },
            { id: "photo_url", dataType: "string" }
         ];
         
         var usersTable = {
             joinOnly : true,
             alias: "Users",
             id: "users",
             columns: userCols
         }
         
         var msgsCols = [
            { id: "from_id", dataType: "string", alias: "From User Id", "foreignKey" : { "tableId" : "users" , "columnId" : "user_id"} },
            { id: "from_name", dataType: "string", alias: "From Name" },
            { id: "from_mention_name", dataType: "string", alias: "From Mention Name" },
            { id: "message", dataType: "string", alias: "Message" },
            { id: "send_date", dataType: "datetime", alias: "Send Date" },
         ]
         
         var msgsTable = {
             alias: "Messages",
             id: "messages",
             columns: msgsCols
         };
         
         var msgWordCloudCols = [
            { id: "from_id", dataType: "string", alias: "From User Id" },
            { id: "from_name", dataType: "string", alias: "From Name" },
            { id: "from_mention_name", dataType: "string", alias: "From Mention Name" },
            { id: "word", dataType: "string", alias: "Word" },
            { id: "send_date", dataType: "datetime", alias: "Send Date" },
         ]
         
         var msgsWordCloudTable = {
             alias: "Messages Word Cloud",
             id: "messages_wordCloud",
             columns: msgWordCloudCols
         };
         
         if (!!tableau.connectionData) {
             // We have a room!
             schemaCallback([msgsTable, msgsWordCloudTable, usersTable]);
         } else {
            schemaCallback([roomsTable, usersTable]);
         }
         
         
     };
    
myConnector.getData = function(table, doneCallback) {
    var pw = tableau.password;

    
    if (table.tableInfo.id == "rooms") {

        var startUrl = "https://api.hipchat.com/v2/room?&max-results=100";
        var processRooms = function(data) {
            var results = [];
            for(var i =0; i<data.items.length; i++) {
                var item = data.items[i];
                var row = {
                    "name" : item.name,
                    "privacy" : item.privacy,
                    "roomId" : item.id,
                    "link" : item.links.self
                };
                
                var getStatsFn = function(roomId, itemRow) {
                    var statsUrl = "https://api.hipchat.com/v2/room/" + roomId  + "/statistics" + "?auth_token=" + tableau.password;
                    
                    return $.ajax({
                        method:"GET",
                        url: statsUrl, 
                        async:false,
                        succes: function(data) {
                            itemRow.messages_sent = data.messages_sent;
                            itemRow.last_active = data.last_active;
                            
                    }});
                }
                
                results.push(row);
            }
            
            table.appendRows(results);
            
            if (!!data.links && !!data.links.next) {
                return data.links.next;
            } else {
                return undefined;
            }
        }
        
        getRows(startUrl, processRooms, doneCallback);
    } else if (table.tableInfo.id == "users") {
        if (!table.isJoinFiltered) {
            tableau.abortWithError("Users must be filtered!");
            return;
        } else {
            var startCount = table.filterValues.length;
            tableau.reportProgress("Loading data for " + startCount + " users.");
            var filterValues = table.filterValues;
            var usersStartUrl = "https://api.hipchat.com/v2/user/";
            var requestNextValue = function() {
                tableau.reportProgress("Fetched " + (startCount - filterValues.length).toString() + " of " + startCount + " users.");
                if (filterValues.length == 0) {
                    // we've read through all of the filterValues we were asked 
                    // to retrieve. We're done with this table!
                    doneCallback();
                } else {
                    // Grab the next value we're supposed to retrieve
                    var val = filterValues.pop();
                    
                    var valUrl = usersStartUrl + val + "?auth_token=" + tableau.password;
                    $.ajax(valUrl, {
                        method: 'GET',
                        dataType: 'json'
                    }).then(function(data) {
                        var row = {};
                        row.user_id = data.id;
                        row.name = data.name;
                        row.mention_name = data.mention_name;
                        row.title = data.title;
                        
                        // last active comes with a really weird format
                        var lastActive = data.last_active;
                        lastActive = lastActive.substring(0, lastActive.indexOf("+"));
                        row.last_active = new Date(lastActive);
                        
                        var parsedCreated = new Date(data.created);
                        row.created = parsedCreated;
                        
                        row.is_online = false;
                        if (!!data.presence) {
                            row.status = data.presence.show;
                            row.status_message = data.presence.status;
                            row.idle_seconds = data.presence.idle;
                            row.is_online = data.presence.is_online;
                        }
                        
                        row.email = data.email;
                        row.photo_url = data.photo_url;
                        
                        table.appendRows([row]);
                        requestNextValue();
                    }).fail(function(failure) {
                        console.log("Something failed");
                        requestNextValue();
                    });
                }
            }
            
            requestNextValue();
        }
        
    } else if (table.tableInfo.id == "messages" || table.tableInfo.id =="messages_wordCloud") {
        var isWordCloud = table.tableInfo.id =="messages_wordCloud";
        
        var roomId = encodeURIComponent(JSON.parse(tableau.connectionData).room);
        date = "recent";
        var startUrl = "https://api.hipchat.com/v2/room/" + roomId + "/history?max-results=1000&reverse=false&date=" + date;
        
        
        var processMessages = function(data) {
            var results = [];
            for(var i =0; i<data.items.length; i++) {
                var item = data.items[i];
                
                if (item.type != "message") {
                    continue;
                }
                
                var row = {
                    "from_id" : item.from.id,
                    "from_name" : item.from.name,
                    "from_mention_name" : item.from.mention_name,
                    "message": item.message,
                    "send_date" : new Date(item.date)
                };
                
                if (isWordCloud) {
                    var tokens = item.message.split(" ");
                    console.log("Split into " + tokens.length + " rows for the message '" + item.message + "'");
                    for(var j in tokens) {
                        if (tokens[j].length > 0) {
                            
                            var duplicateRow = $.extend(true, {}, row);
                            duplicateRow.send_date = new Date(item.date);
                            duplicateRow.word = tokens[j];
                            results.push(duplicateRow);
                        }
                    }
                    
                } else {
             
                    results.push(row);
                }
            }
            
            table.appendRows(results);
            
            if (data.items.length > 0) {
                // we keep going
                var newDate = data.items[data.items.length - 1].date;
                tableau.reportProgress("Fetched messages back until " + newDate);
                newDate = newDate.replace("+", "%2B");
                var newUrl = "https://api.hipchat.com/v2/room/" + roomId + "/history?max-results=1000&reverse=false&date=" + newDate;
                return newUrl;
            } else {
                return undefined;
            }
        }
        
        getRows(startUrl, processMessages, doneCallback);
    }
};

    var rooms = [];
    var loadRooms = function() {
        rooms = [];

        var setupSelect = function() {
            $('#sel1').empty();
            $.each(rooms, function (i, item) {
                $('#sel1').append($('<option>' + item + '</option>', { 
                    value: item,
                    text: item
                }));
            });
            $('#sel1').selectpicker('refresh');
        }

        var startUrl = "https://api.hipchat.com/v2/room?&max-results=1000";
        var processRooms = function(data) {
            for(var i =0; i<data.items.length; i++) {
                var name = data.items[i].name;
                rooms.push(name);
            }
            
            if (!!data.links && !!data.links.next) {
                return data.links.next;
            } else {
                return undefined;
            }
        }

        getRows(startUrl, processRooms, setupSelect);
    }

     setupConnector = function() {
        tableau.connectionName = "HipChat Connector";
        tableau.password = $("#access_token").val();
        var connData = {
            "room" : $("#sel1 option:selected").text(),
            "rooms" : rooms
        };

        tableau.connectionData = JSON.stringify(connData); // $("#sel1 option:selected").text();
        tableau.authType = "basic";
        tableau.submit();
     };

    tableau.registerConnector(myConnector);

    var updateUi = function() {
        var hasAccessToken = !!$("#access_token").val();
        $("#load_rooms_button").prop("disabled", !hasAccessToken);
        var hasRoom = !!$("#sel1 option:selected").text();
        $("#submitButton").prop("disabled", !hasRoom);
    }

    $(document).ready(function() {
        updateUi();

        $("#submitButton").click(function() { // This event fires when a button is clicked
            setupConnector();
        });
        $('#inputForm').submit(function(event) {
            event.preventDefault();
            setupConnector();
        });

        $("#load_rooms_button").click(function() {
            tableau.password = $("#access_token").val();
            loadRooms();
        });
        $('#sel1').change(updateUi);
        $("#access_token").keyup(updateUi);
        $("#access_token").val(tableau.password);

        updateUi();
    });
})();
