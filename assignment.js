

//1.Show all the EPL teams involved in the season.
db.games.distinct("HomeTeam")

//2.How many matches were played on Mondays?
db.games.aggregate([
    { $project : { date : { $dayOfWeek : {$dateFromString: { dateString: "$Date", format: "%d/%m/%Y" } } } } },
    { $match : { date : { $eq : 2} } },
    { $group : { _id : "GamesOnMonday", count : { $sum : 1 } } }
])

//3.Display the total number of goals “Liverpool” had scored and conceded in the season.
db.games.aggregate([
    { $facet : { 
        home : [ 
            { $match : { HomeTeam : "Liverpool" } }, 
            { $group : { _id : null, goalsScored : { $sum : "$FTHG" }, goalsConceded : { $sum :"$FTAG" } } }
        ],
        away : [ 
            { $match : { AwayTeam : "Liverpool" } }, 
            { $group : { _id : null, goalsScored : { $sum : "$FTAG" }, goalsConceded : { $sum :"$FTHG" } } }
        ],  
      }
    }, 
    { $unwind : "$home" },
    { $unwind : "$away" },
    { $project : { 
        GoalsScored : { $add : ["$home.goalsScored" , "$away.goalsScored"] }, 
        GoalsConceded : { $add : ["$home.goalsConceded" , "$away.goalsConceded"] } 
        } 
    }     

])

//4.Who refereed the most matches?
db.games.aggregate( [ 
    { $group : { _id : "$Referee", count: { $sum : 1 } } },
    // { $group : { _id : null , maxCount: { $max : "$count" } } } ------ Sort By Count
    { $sort : { count : -1 } },
    { $limit : 1 }
])

//5.Display all the matches that “Man United” lost.
db.games.find( {
    $or : [ 
        { $and: [ { HomeTeam : "Man United" }, { FTR : "A" } ] },
        { $and: [ { AwayTeam : "Man United" }, { FTR : "H" } ] }
    ]
})

//6.Write a query to display the final ranking of all the teams based on their total points.

db.games.mapReduce(
    function() {
        switch(this.FTR) {
            case "H" : emit(this.HomeTeam, 3)
                       break;
            case "A" : emit(this.AwayTeam, 3)
                       break;
            case "D" : emit(this.HomeTeam, 1)
                       emit(this.AwayTeam, 1)
                       break;
        }
    },
    function (key, values) { return Array.sum(values) },
    {
        out : { inline : 1 }
    }
)
db.points.aggregate( { $sort: { "value" : -1}} );

// other method

var x = String(db.games.distinct("HomeTeam"));
var teams = x.split(",");
var points = {};

function getPoints(team){
     var points = db.games.aggregate([
        { $facet : { 
            wins : [ 
                { $match : { $or : [ {$and : [{HomeTeam : team}, {FTR : "H"}]}, {$and : [{AwayTeam : team}, {FTR : "A"}]} ]} }, 
                { $group : { _id : null , count: { $sum: 3 } } },
            ],
            homeDraw : [ 
                { $match : { $and : [ { HomeTeam : team }, { FTR : "D" } ] } }, 
                { $group : { _id : null , count: { $sum: 1 } } },
            ],
            awayDraw : [ 
                { $match : { $and : [ { AwayTeam : team }, { FTR : "D" } ] } }, 
                { $group : { _id : null , count: { $sum: 1 } } },
            ]

          }
        }, 
        { $unwind : "$wins" },
        { $unwind : { path: "$homeDraw", preserveNullAndEmptyArrays: true } },
        { $unwind : { path: "$awayDraw", preserveNullAndEmptyArrays: true } },
        { $set :
              { 
                  "home" : { $ifNull : ["$homeDraw.count" , 0] },
                  "away" : { $ifNull : ["$awayDraw.count" , 0] } 
              }
                  
         },
         { $project : { 
                points : { $add : [ "$wins.count", "$home" , "$away" ] }
            }
         }

    ]).toArray();   
    return points[0]["points"];
}

for (var team in teams){
    points[teams[team]] = getPoints(teams[team]);
}

printjson(points);

