---
layout: post
title: "Self-joins: finding different metrics with different filters per metric"
---
A colleague recently had a great question about joins, running into an implementation quirk of Kusto around default join flavours. We'll get to that later. To start with, here was their question: 
> I'm using the Change Tracking solution in Log Analytics to track the latest state of the registry on my machines. There are two keys that I care about:
>
> - HKLM\Software\Contoso\UpdateGroup
> - HKLM\Software\Contoso\ServerRole  
>  
> <br/>The relevant data looks as follows:  
>  
| TimeGenerated | Computer | ConfigDataType   | RegistryKey | ValueName | ValueData |  
| ------------- | -------- | -------------- | ----------- | --------- | --------- |  
| 2018-01-01 | ComputerName | Registry | HKEY_LOCAL_MACHINE\SOFTWARE\Contoso | UpdateGroup | UpdateGroup1 |  
| 2018-01-02 | ComputerName | Registry | HKEY_LOCAL_MACHINE\SOFTWARE\Contoso | UpdateGroup | UpdateGroup2 |  
| 2018-01-01 | ComputerName | Registry | HKEY_LOCAL_MACHINE\SOFTWARE\Contoso | ServerRole | DC |  
| 2018-01-05 | ComputerName | Registry | HKEY_LOCAL_MACHINE\SOFTWARE\Contoso | ServerRole | NotDC |  
> 
>  <br/>
> In the above example, the machine "ComputerName" was in upgrade group 1, but is as of the latest entry in upgrade group 2. Similarly, it was in the "DC" server role, but has since become a "NotDC".  
>
> I want to create a query to find any machines that as of their latest entry are in upgrade group "E" and have the server role "DC".

So where do we start with this query? Well, the first thing I need is some data I can experiment with. Let's use a _datatable()_ here. Datatables in general are a great way to quickly mock some data up. I use them almost all the time!  

{% highlight powershell linenos %}
let MockedConfigurationData = datatable(TimeGenerated:datetime, Computer:string, ConfigDataType:string, RegistryKey:string, ValueName:string, ValueData:string)
[
    datetime('2018-08-24T20:42:26.870'), "Machine1", "Registry", @"HKEY_LOCAL_MACHINE\SOFTWARE\Contoso", "UpdateGroup", "E",
    datetime('2018-08-23T20:42:26.870'), "Machine1", "Registry", @"HKEY_LOCAL_MACHINE\SOFTWARE\Contoso", "UpdateGroup", "C",
    datetime('2018-08-24T20:42:26.870'), "Machine1", "Registry", @"HKEY_LOCAL_MACHINE\SOFTWARE\Contoso", "ServerRole", "DC",
    datetime('2018-08-22T20:42:26.870'), "Machine1", "Registry", @"HKEY_LOCAL_MACHINE\SOFTWARE\Contoso", "ServerRole", "notDC",
    datetime('2018-08-24T20:42:26.870'), "Machine2", "Registry", @"HKEY_LOCAL_MACHINE\SOFTWARE\Contoso", "UpdateGroup", "F",
    datetime('2018-08-23T20:42:26.870'), "Machine2", "Registry", @"HKEY_LOCAL_MACHINE\SOFTWARE\Contoso", "UpdateGroup", "E",
    datetime('2018-08-24T20:42:26.870'), "Machine2", "Registry", @"HKEY_LOCAL_MACHINE\SOFTWARE\Contoso", "ServerRole", "DC",
    datetime('2018-08-22T20:42:26.870'), "Machine2", "Registry", @"HKEY_LOCAL_MACHINE\SOFTWARE\Contoso", "ServerRole", "notDC"
];
MockedConfigurationData
{% endhighlight %}

This will have everything we need to get going: a couple of machines with some edge cases, one of which ultimately matches the criteria we need. Let's run the query... looks good! The next logical step is to pick out latest update group and server role for each machine. _Argmax()_ is key here - it lets us pick a dimension to maximize (in this case, time), and then retrieve all the data for the row that contains that maximized dimension. I'll include a more understandable explanation after we take a look at the queries. I'm excluding the full MockedConfigurationData definition for the sake of readability - simply copy/paste it back in from above if you're following along!  

Here's the query for update groups:  
{% highlight powershell linenos %}
let MockedConfigurationData = datatable(...)
[
    ...
];
MockedConfigurationData  
| where ConfigDataType == "Registry"
| where ValueName == "UpdateGroup"
| summarize arg_max(TimeGenerated, *) by Computer
{% endhighlight %}
Just as we'd expect, this query returns a ValueData of "E" for Machine1, and "F" for Machine2. I promised I'd talk a bit more about _argmax()_. Let's break this query down line-by-line:  

- *Lines 6-7*: from the main set of data, select only that which is a ConfigDataType of Registry and a ValueName of UpdateGroup. This discards file changes, server roles, etc.  
- *Line 8*: the most interesting part of this query. We can think about it as follows:
  - For every computer in the dataset passed into the summarize command (only UpdateGroup data at this point), find the row that has the maximum TimeGenerated value. Since we're down to only UpdateGroup data at this point, this will tell us the latest time an UpdateGroup was reported for every machine. 
  - Where _argmax()_ becomes really cool is the second parameter: "\*". This \* tells Kusto to return every other column in the row where we found the highest TimeGenerated. 
 Think about looking at a spreadsheet you printed to do this manually. You'd look through the "TimeGenerated" column to find the highest value, and then highlight that entire row so that you can further analyze it. This is exactly what _argmax()_ does!  
  
<br/>Still following along? Try to use just the datatable to come up with the same query, but for the ServerRole. Once you're done, compare it to the following working version:
{% highlight powershell linenos %}
let MockedConfigurationData = datatable(...)
[
    ...
];
MockedConfigurationData  
| where ConfigDataType == "Registry"
| where ValueName == "ServerRole"
| summarize arg_max(TimeGenerated, *) by Computer
{% endhighlight %}

Those look a bit similar, don't they... just one line difference between them. That means that this query is a great candidate for us to use _materialize()_.  

Think of materialize as follows: we'll take some sub-query, run it, store the results in memory, and assign an alias to it. Then, anytime for the rest of the query that we need to reference the results of this sub-query, we'll use the alias to pull the results from memory, rather than recalculating them every single time!  

So what do we apply _materialize()_ on for this query? As we discussed, these will be the common parts. In this case, the common parts are the fact that we're always looking at the "Registry" ConfigDataType in the MockedConfigurationData table. Here's the query:
{% highlight powershell linenos %}
let MockedConfigurationData = datatable(...)
[
    ...
];
let baseData = materialize(
    MockedConfigurationData 
    | where ConfigDataType == "Registry"
);
baseData
{% endhighlight %}

Now we can rebuild the two sub-queries for server role and update group with this baseData alias: 
{% highlight powershell linenos %}
let MockedConfigurationData = datatable(...)
[
    ...
];
let baseData = materialize(
    MockedConfigurationData 
    | where ConfigDataType == "Registry"
);
let latestServerRoles = ()
{
    baseData
    | where ValueName == "ServerRole"
    | summarize arg_max(TimeGenerated, *) by Computer
};
let latestUpdateGroups = ()
{
    baseData
    | where ValueName == "UpdateGroup"
    | summarize arg_max(TimeGenerated, *) by Computer
};
{% endhighlight %}

This query won't actually run since we're not calling any data source in our main query - in fact, we don't have a main query at all. Let's think about what should go there: we now have two subqueries, one that per computer shows us the latest server role, and another that per computer shows us the latest update group. We want to pull these together so that our data looks something like this:

| Computer | LatestUpdateGroup | LatestServerRole |
| --- | --- | --- |
| Computer 1 | UG1 | SR1 |
| Computer 2 | UG1 | SR2 |
| ... | ... | ... |
  
<br/>This "imagine how the output should look like" trick, by the way, is an awesome way to go about solving complex problems like this. I remember the very first programming class I took, the teacher beat into us the need to add a comment saying "inputs: [...], outputs: [...]" before any method, taking marks off if we didn't. Same thing here. I digress...  

To get our data looking like we want given the building blocks we have, we clearly need a join. If you're not familiar with joins, check out the [Wikipedia article](https://en.wikipedia.org/wiki/Join_(SQL)) that explains them. They use T-SQL notation there, but the concepts apply here just as well. Our key (the thing that's common between the tables we're joining) will be the computer name. Let's try to just write the join without any further modifications:
{% highlight powershell linenos %}
let MockedConfigurationData = datatable(...)
[
    ...
];
let baseData = materialize(
    MockedConfigurationData 
    | where ConfigDataType == "Registry"
);
let latestServerRoles = ()
{
    baseData
    | where ValueName == "ServerRole"
    | summarize arg_max(TimeGenerated, *) by Computer
};
let latestUpdateGroups = ()
{
    baseData
    | where ValueName == "UpdateGroup"
    | summarize arg_max(TimeGenerated, *) by Computer
};
latestServerRoles
| join (latestUpdateGroups) on $left.Computer == $right.Computer
{% endhighlight %}

This isn't bad - we have the data we need, and it's correct... it just looks a bit ugly:  

| Computer | TimeGenerated | ConfigDataType | RegistryKey | ValueName | ValueData | Computer1 | TimeGenerated1 | ConfigDataType1 | ... |  
| --- | --- | --- | --- | --- | --- | --- | --- | --- | ---|  
| Machine 1 | 2018-08-24 | Registry | HKLM\Software\Contoso | ServerRole | DC | Machine 1 | 2018-08-24 | Registry | HKLM\Software\Contoso | ...|  
| Machine 2 | ... | ... | ... | ... | ... | ... | ... | ... | ... |  

<br/>Essentially what's happening is Kusto not knowing what to call duplicate columns. It adds "1", "2", etc to them to distinguish ones with the same names. But we really don't need a bunch of this information to be duplicated at all, and can rename some others to make things clearer. _project()_ to the rescue!: (since this is our final solution for this problem, pasting the whole thing, datatable included):
{% highlight powershell linenos %}
let MockedConfigurationData = datatable(TimeGenerated:datetime, Computer:string, ConfigDataType:string, RegistryKey:string, ValueName:string, ValueData:string)
[
    datetime('2018-08-24T20:42:26.870'), "Machine1", "Registry", @"HKEY_LOCAL_MACHINE\SOFTWARE\Contoso", "UpdateGroup", "E",
    datetime('2018-08-23T20:42:26.870'), "Machine1", "Registry", @"HKEY_LOCAL_MACHINE\SOFTWARE\Contoso", "UpdateGroup", "C",
    datetime('2018-08-24T20:42:26.870'), "Machine1", "Registry", @"HKEY_LOCAL_MACHINE\SOFTWARE\Contoso", "ServerRole", "DC",
    datetime('2018-08-22T20:42:26.870'), "Machine1", "Registry", @"HKEY_LOCAL_MACHINE\SOFTWARE\Contoso", "ServerRole", "notDC",
    datetime('2018-08-24T20:42:26.870'), "Machine2", "Registry", @"HKEY_LOCAL_MACHINE\SOFTWARE\Contoso", "UpdateGroup", "F",
    datetime('2018-08-23T20:42:26.870'), "Machine2", "Registry", @"HKEY_LOCAL_MACHINE\SOFTWARE\Contoso", "UpdateGroup", "E",
    datetime('2018-08-24T20:42:26.870'), "Machine2", "Registry", @"HKEY_LOCAL_MACHINE\SOFTWARE\Contoso", "ServerRole", "DC",
    datetime('2018-08-22T20:42:26.870'), "Machine2", "Registry", @"HKEY_LOCAL_MACHINE\SOFTWARE\Contoso", "ServerRole", "notDC"
];
let baseData = materialize(
    MockedConfigurationData 
    | where ConfigDataType == "Registry"
);
let latestServerRoles = ()
{
    baseData
    | where ValueName == "ServerRole"
    | summarize arg_max(TimeGenerated, *) by Computer
    | project Computer, TimeGeneratedServerRole=TimeGenerated, LatestServerRole=ValueData
};
let latestUpdateGroups = ()
{
    baseData
    | where ValueName == "UpdateGroup"
    | summarize arg_max(TimeGenerated, *) by Computer
    | project Computer, TimeGeneratedUpdateGroup=TimeGenerated, LatestUpdateGroup=ValueData
};
latestServerRoles
| join kind = fullouter (latestUpdateGroups) on $left.Computer == $right.Computer
| project-away Computer1
{% endhighlight %}

Much better:  

| Computer | TimeGeneratedServerRole | LatestServerRole | TimeGeneratedUpdateGroup | LatestUpdateGroup |  
| --- | --- | --- | --- | --- |
| Machine1 | ... | DC | ... | E |  
| Machine2 | ... | DC | ... | F |

<br/>Oh, and by the way, if you actually need to filter this down to a particular Server Role and Update Group, all it takes is a simple where filter:  
{% highlight powershell %}
...
| where LatestServerRole == "DC" and LatestUpdateGroup == "E"
{% endhighlight %}

So now about that join quirk. We broke our data up into individual _argmax()_ blocks, meaning that in each of our sub-queries, each computer would by definition only have one row associated with it. The author originally tried to do everything in one big query where multiple rows per Computer could exist. They were confused as to why the results seemed non-deterministic: sometimes, the results would be correct, and other times incorrect data would show up, almost as if _argmax()_ isn't working. Turns out, Kusto's default join type is "inner with dedupe". This means that it'll act like a classic inner join (ie: values for both sides have to be present for the combination to be included in the result set), but only one row is selected on each side, rather than all possible permutations, as anyone coming from T-SQL-land would expect. It's a small quirk, just something you should watch out for!  

That's about it for this problem. We covered a lot of ground. Some key takeaways:  

- Operators of interest that we introduced and explored: _argmax()_, _let_, _materialize_, _join_, _datatable_  
- Break your problem up into smaller building blocks  
- Visualize the output you want to see, work your way back to it  
- Be aware of the default Kusto join flavour (inner-with-dedupe)  
  