$(function() {

  var locDateStr=new Date().toLocaleString();
  console.log( "JS: locDateStr=",locDateStr);
  $("#timeNowClient").html(locDateStr)
 
//==========  BUTTONS =========
$('#accSetup').on('click', function() {

   var starProdId=document.getElementById("inpProdId").value;
   var ageSecH=document.getElementById("inpAgeSec").value;
   var refTimeHour=document.getElementById("inpRefTime").value;
    console.log( "JS: acc STAR prodId=", starProdId, "  ageSecH=",ageSecH," refTimeHour=",refTimeHour);  
   $("#starProdId").html( starProdId)
   $("#ageSecH").html( ageSecH)
   $("#refTimeHour").html(refTimeHour)

});

});