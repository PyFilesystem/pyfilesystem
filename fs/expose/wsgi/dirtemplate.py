template = """
<html>

<head>
<title>${path}</title>
<style type="text/css">

body {
    font-family:Arial, Verdana;
    margin:0px;
    padding:0px;
}

table.dirlist {
    margin:0 auto;
    font-size:13px;
    color:#666;
    min-width:960px;
}

table.dirlist tr.r1 {
    background-color:#f4f4f4;
}

table.dirlist td {
    padding:6px 12px;
    margin:0px;
}

table.dirlist td a.link-dir {
    font-weight:bold;
}

table.dirlist td a {
    text-decoration:none;
}

table.dirlist td a:hover {
    text-decoration:underline;
}

table.dirlist tr {
    padding:4px;
    margin:0px;
}

table.dirlist tr:hover {
    background-color:#e9e9e9;
}


</style>

</head>

<body>

<div class="dirlist-container">

<table class="dirlist">

    <thead>
        <tr>
            <th>File/Directory</th>
            <th>Size</th>
            <th>Created Date</th>
        </tr>
    </thead>
    <tbody>
        % for i, entry in enumerate(dirlist):
        <tr class="${entry['type']} r${i%2}">
            <td><a class="link-${entry['type']}" href="${ entry['path'] }">${entry['name']}</a></td>
            <td>${entry['size']}</td>
            <td>${entry['created_time']}</td>
        </tr>
        % endfor
    </tbody>

</table>

</div>

</body>

</html>

"""