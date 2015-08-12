$(document).ready(function() {
  $(".panel-heading").click(function() {
     $(this).nextAll('table').fadeToggle('slow');
  });
});
