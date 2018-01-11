/*jslint browser: true*/
/*global $*/


$(document).ready(function () {
    $('#serverside_table').DataTable({
        bProcessing: true,
        bServerSide: true,
        sPaginationType: "full_numbers",
        lengthMenu: [[10, 25, 50, 100], [10, 25, 50, 100]],
        bjQueryUI: true,
        sAjaxSource: '/fintweet/serverside_table_data',
        columns: [
            {"data": "Cashtag"},
            {"data": "Count"}
            // {"data": "Column C"},
            // {"data": "Column D"}
        ]
    });
});