function pageLoadFunction() {
  //alert("Hello")



  var locDateStr=new Date().toLocaleString();
  console.log( "JS: locDateStr=",locDateStr);
  $("#timeNowClient").html(locDateStr)
 

}; //------ end of function pageLoadFunction

//=============================
//=============================
//=============================

function pagePlotFunction() {

//-----------  DRAWING CODE --------
 var useUtc=false; // time axis for display
 Highcharts.setOptions({ global: {  useUTC: useUtc }});
 console.log( "JS: ready at top, useUtc=",useUtc);
 var date0=Date.now();



//--------- A A A A A -------------
    $('#containerPLA').highcharts({
        title: { text: 'some titel ', x: -20  },
        //chart: { type: 'spline' },
        xAxis: {  type: 'datetime', useUTC: useUtc,
            dateTimeLabelFormats: {   day: '%e / %b / %y'  },
            title: {   text: 'Date ( UTC='+useUtc+')' } },
        yAxis: {  title: {   text: 'number of jobs' },  min: 0
        },
        tooltip: {    headerFormat: '<b>{series.name}</b><br>',
            pointFormat: '{point.x:%H:%M:%S-%b.%e}: {point.y:%.2f} cores'  },
        series: []
    }); // end of #containerPLA




//.................................  
//....  the action  buttons ......
//.................................  


$('#askFHB').on('click', function() {
       askServerFHB();
});


$('#utcOff').on('click', function() {
       useUtc=false;
       fullReDraw();
});


$('#utcOn').on('click', function() {
       useUtc=true;
       fullReDraw();
});

var chart = $('#containerPLA').highcharts();

function fullReDraw() {
   console.log( "JS: new utc=", useUtc);
   $("#useUtc").html(useUtc ? 'true1':'false1');
   Highcharts.setOptions({ global: {  useUTC: useUtc }});
   chart.xAxis[0].update({ title:{ text: 'Date  UTC='+useUtc } });
   chart.redraw();
}


//.......................................
// .... talk to python-flask on server....
//.......................................


function askServerFHB() { //  mongoDB full history request
   $("#mgDbComState").html("requesting-FHB...")
   console.log("JS: begNumSer=",chart.series.length);	
   chart.showLoading('askServerFHB ...');
   new Ajax.Request( '../askFH', {  method:  'get',
      onSuccess:  function(response){
         $("#mgDbComState").html("received-FHB")
         var jsn = JSON.parse(response.responseText);
         var dbInfo=jsn['dbInfo'];
         var dbSers=jsn['dbSers'];

	 for (var key in dbSers) {
 	     //console.log("FHB : key=",key);
	     addSeries(key);
	     replaceSeries(key, dbSers[key])
	     //break;
	 } 

       $( "#mgDbQueryCnt" ).html( dbInfo['queryCnt']) // OLD?

	// title of the canvas
	chart.setTitle({text:dbInfo['name']});

        console.log("JS: finNumSer=",chart.series.length);	
         chart.hideLoading();
         chart.redraw();
      }, onFailure:  function(){
          console.log("JS: 2bad-res:",response.responseText);
          $("#mgDbComState").html("failed-FH")
        alert('ERROR');
      }
  });// end of Ajax
console.log( "JS: ajax get askServerFHB end");
}



$('#getChartInfo').on('click', function() {

   console.log("CI: numSer=",chart.series.length);
   for ( var j=0; j<chart.series.length; j++) {
      console.log("CI: ser=",j," name=",chart.series[j].name," len=",chart.series[j].data.length);
   }
});


//------------------

function addSeries(nameX) {
   //console.log( "aSR: nameX=",nameX);
   var isKnown=false;
   var j=0	;
   for (  ; j<chart.series.length; j++) {
      var isKnown=nameX==chart.series[j].name;
      //console.log("aSR: ser=",j," name=",chart.series[j].name, "isKnown=",isKnown);
      if( isKnown) break;
   }

 if ( !isKnown ) { // add series
    chart.addSeries({  name: nameX, data: []}, false );
    //console.log("aSR: add ser=",nameX, " j=",j);
   } 
}

//------------------

function replaceSeries(nameX, pointL) {
   //console.log( "rSR: nameX=",nameX,pointL[0]);
   var isKnown=false;
   var j=0	;
   for (  ; j<chart.series.length; j++) {
      var isKnown=nameX==chart.series[j].name;
      //console.log("rSR: ser=",j," name=",chart.series[j].name," len=",chart.series[j].data.length, "isKnown=",isKnown);
      if( isKnown) break;
   }
 var dataLen=pointL.length;
 if ( isKnown ) { // replace series
  //console.log("rSR: swap ser=",nameX," len=",dataLen, " j=",j);
 for (var i = 0; i < dataLen; i++) {
            chart.series[j].addPoint([Date.parse(pointL[i][0]),pointL[i][1]]);
         }
    console.log("rSR: swap ser=",nameX," new Len=",chart.series[j].data.length);
  }
}




};
