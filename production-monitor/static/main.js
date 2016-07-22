$(function() {

  var locDateStr=new Date().toLocaleString();
  console.log( "JS: locDateStr=",locDateStr);
  $("#timeNowClient").html(locDateStr)
 	
	//{{ jan26|tojson|safe }}


//----- button GET Data
  $('#get88').on('click', function() {
    var entry = this;
    $("#kawa12").html(55) // modify content 
    myState=2		
    console.log("JS: get88 entry=",entry," myState=",myState);
    var post_id = $(this).find('submit').attr('id');
    console.log("JS: get88b id=",post_id);
  });


});