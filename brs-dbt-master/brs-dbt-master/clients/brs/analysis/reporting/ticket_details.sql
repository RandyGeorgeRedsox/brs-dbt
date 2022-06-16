SELECT 
AC.AccountId,
AC.AccountName,
OP.OperatorFormattedName,
T.TicketId,
BT.BuyerTypeGroupCd,
BT.BuyerTypeGroupDesc,
BT.BuyerTypeCd,
BT.BuyerTypeDesc,
E.EventCd,
CASE when ZZ.EventDate is NULL then EE.EventDate else ZZ.EventDate end as EventDate, /* ZZ.Event date used to solve Carmine issue with game dates */
E.EventDesc,
EE.EventCd as UsageEventCd,
D.FullDate AS 'Transaction Date',
PS.PriceScaleCd,
P.PackageCd,
PL.PackageLineOrderOriginCd,
DL.DeliveryMethodDesc,
DL.DeliveryMethodCd,
S.SeatNum,
S.SeatRow,
S.SeatSectionCd,
A.AgencyCd,
A.AgencyDesc,
ZZ.AwayTeamAbbr,
ZZ.AwayTeam,
ZZ.Comment,
ZZ.TierNumber,
ZZ.HomeStandNumber,
ZZ.GameTimeLocal,
ZZ.GameDate,
ZZ.VenueName,
POL.PatronOrderLineTransactionTypeCd,
RRD.Tableau_Run_Date,
MA.CenterX,
MA.CenterY,

/*Useful to avoid cartesians. Please do not modify unless you know what you're doing */

max(isnull(ZZ.BudgetValue,0)) as BudgetValue,
max(isnull(ZZ.BudgetRevenue,0)) as BudgetRevenue,
max(isnull(ZZ.SoldoutValue,0)) as SoldoutValue,

max(isnull(ZZ.TotalBudgetValue,0)) as TotalBudgetValue,
max(isnull(ZZ.TotalBudgetRevenue,0)) as TotalBudgetRevenue,
sum(isnull(POT.TicketCnt,0)) AS 'Tickets',
sum(isnull(POT.TicketRemoveCnt,0)) as TicketRemoveCnt,
sum(isnull(POT.TicketPrice,0)) - sum(isnull(SC.ServiceChargeAmt,0)) AS 'Revenue'

FROM FACT.PatronOrderTransactionTicketDetail POT


/* Logic to deal with Slowly Changing Dimension affected rainout games
 * Event Codes needed to get correct revenues and ticket counts.
 * Please do not modify unless you know what you're doing 
 */

inner join 
(SELECT DISTINCT 
CASE WHEN B.EventCd IS NULL THEN A.EventCd 
WHEN A.EventCd IS NULL THEN B.EventCd 
WHEN A.EventCd <> B.EventCd THEN B.EventCd
ELSE A.EventCd
END AS EventCd,
CASE WHEN B.EventCd IS NULL THEN A.EventDesc
WHEN A.EventCd IS NULL THEN B.EventDesc
WHEN A.EventCd <> B.EventCd THEN A.EventDesc
ELSE A.EventDesc
END AS EventDesc,
CASE WHEN B.EventCd IS NULL THEN A.EventKey 
WHEN A.EventCd IS NULL THEN B.EventKey
WHEN A.EventCd <> B.EventCd THEN A.EventKey
ELSE A.EventKey
END AS EventKey,
CASE WHEN B.EventCd IS NULL THEN A.EventDate
WHEN A.EventCd IS NULL THEN B.EventDate
WHEN A.EventCd <> B.EventCd THEN B.EventDate
ELSE A.EventDate
END AS EventDate
FROM 
(select EventCd, EventDesc, EventKey, EventDate, EventId
from DIM.Event 
WHERE (EventCd LIKE '%RS%'
or EventCd LIKE '%LF%'
or EventCd LIKE '%LC%'
or EventCd LIKE '%CA%'
or EventCd LIKE '%GM%'
or EventCd LIKE 'RS%T%LFE'
)
and EventCd not LIKE 'RS%VE'
) A

left join

(SELECT EventCd, EventDesc, EventKey, EventDate, EventId
from DIM.Event
WHERE CONVERT(NVARCHAR(50), EventId) + CONVERT(NVARCHAR(50), EventCd)
 IN (
SELECT CONVERT(NVARCHAR(50), EventId) + CONVERT(NVARCHAR(50), MAX(EventCd))
FROM DIM.Event
GROUP BY eventid
HAVING COUNT(DISTINCT EventCd) > 1)) B

ON B.eventid = A.eventid) E
on POT.EventKey = E.EventKey

/*Getting only this year's data. Please do not modify. Data is meant to be at ticket level and adding more years will make query very long*/

inner JOIN
(SELECT DISTINCT 
CASE WHEN BB.EventCd IS NULL THEN AA.EventCd 
WHEN AA.EventCd IS NULL THEN BB.EventCd 
WHEN AA.EventCd <> BB.EventCd THEN BB.EventCd
ELSE AA.EventCd
END AS EventCd,
CASE WHEN BB.EventCd IS NULL THEN AA.EventKey 
WHEN AA.EventCd IS NULL THEN BB.EventKey
WHEN AA.EventCd <> BB.EventCd THEN AA.EventKey
ELSE AA.EventKey
END AS EventKey,
CASE WHEN BB.EventCd IS NULL THEN AA.EventDate
WHEN AA.EventCd IS NULL THEN BB.EventDate
WHEN AA.EventCd <> BB.EventCd THEN BB.EventDate
ELSE AA.EventDate
END AS EventDate
FROM 
(select EventCd, EventDate, EventKey, EventId
from DIM.Event
WHERE (EventCd LIKE '%RS%'
or EventCd LIKE '%LF%'
or EventCd LIKE '%LC%'
or EventCd LIKE '%CA%'
or EventCd LIKE '%GM%'
or EventCd LIKE 'RS%T%LFE'
)
and EventCd not LIKE 'RS%VE'
AND (year(EventDate) = case when month(getdate()) < 11 then year(getdate()) else year(getdate())+1 end
)) AA

LEFT join

(SELECT EventCd, EventKey, EventDate, EventId
from DIM.Event
WHERE CONVERT(NVARCHAR(50), EventId) + CONVERT(NVARCHAR(50), EventCd)
 IN (
SELECT CONVERT(NVARCHAR(50), EventId) + CONVERT(NVARCHAR(50), MAX(EventCd))
FROM DIM.Event
GROUP BY eventid
HAVING COUNT(DISTINCT EventCd) > 1)) BB
ON BB.eventid = AA.eventid) EE

ON POT.UsageEventKey = EE.EventKey

inner join 
(select BuyerTypeCd, BuyerTypeDesc, BuyerTypeGroupCd, BuyerTypeGroupDesc, BuyerTypeKey from DIM.BuyerType) BT
on POT.BuyerTypeKey = BT.BuyerTypeKey

inner join 
(select accountid, accountname, AccountKey from DIM.Account) AC
on POT.AccountKey = AC.AccountKey

inner join 
(select OperatorKey, OperatorFormattedName from DIM.Operator) OP
on POT.OperatorCreateKey = OP.OperatorKey

inner join 
(select PackageKey, PackageCd--, PackageFullSeasonEqu 
from DIM.Package) P
on POT.PackageKey = P.PackageKey

left join 
(select PackageLineKey, PackageLineOrderOriginCd from [DIM].[PackageLine]) PL
on POT.PackageLineKey = PL.PackageLineKey

inner join 
(select FullDate, DateKey from DIM.Date) D
on POT.TransactionDateKey = D.DateKey

inner join 
(select TicketId, TicketKey from DIM.Ticket) T
on POT.TicketKey = T.TicketKey

inner join 
(select SeatNum, SeatRow, SeatSectionCd, SeatKey
from DIM.Seat) S
on POT.SeatKey = S.SeatKey

inner join 
(select PatronOrderLineTransactionTypeCd, PatronOrderLineKey
from DIM.PatronOrderLine) POL
on POT.PatronOrderLineKey = POL.PatronOrderLineKey

LEFT JOIN 

(SELECT SCD.TicketKey, SUM(SCD.ServiceChargeAmt) AS ServiceChargeAmt FROM 
FACT.PatronOrderServiceChargeDetail SCD

INNER JOIN DIM.ServiceChargeItem SCI
on SCI.ServiceChargeItemKey = SCD.ServiceChargeItemKey
AND SCI.ServiceChargeIncludeInTicketPrice = 1
AND SCI.EDWIsCurrent = 1

GROUP BY SCD.TicketKey 
) SC
ON POT.TicketKey =  SC.TicketKey

inner join DIM.Delivery DL
on POT.DeliveryKey = DL. DeliveryKey

inner join (select PriceScaleCd, PriceScaleKey from DIM.PriceScale) PS
on POT.PriceScaleKey = PS.PriceScaleKey

left join (select AgencyKey, AgencyCd, AgencyDesc from DIM.Agency) A
on POT.AgencyKey = A.AgencyKey

left join 


/* Budget/Carmine data joins. Please consult George Hom for details */

(select distinct
 S.AwayTeamAbbr,
 S.AwayTeam,
 G.UpdBudgetValue as BudgetValue,
 G.UpdBudgetRevenue as BudgetRevenue,
 S.Comment,
 S.TierNumber,
 S.HomeStandNumber,
 G.UpdSoldoutValue as SoldoutValue,
 S.GameDate,
 G.EventStartTime as GameTimeLocal,
 CASE WHEN S.AwayTeamAbbr <> 'BOS' THEN 'Fenway Park' END as VenueName,
 YEAR(G.EventDate) as [Year],
 G.UpdTrueSellout as TrueSelloutValue,
 EEE.EventCd,
 EEE.EventDate as EventDate,
 YY.TotalBudgetRevenue,
 YY.TotalBudgetValue

 from [FACT].[GameBudget] G
     inner join [DIM].[Schedule] S
          on S.ScheduleKey = G.ScheduleKey
		  and S.EDWIsCurrent = 1
	 inner join [DIM].[Event] EEE
          on EEE.EventKey = G.EventKey
          and EEE.EventCd <> 'DEF'
          and EEE.EDWIsCurrent = 1
	 /* Select statement below avoids including budget data for rainout games by leveraging on the GamePK */
     inner join 
     (select YEAR(GGG.GameDate) as [Year], sum(GGG.UpdBudgetRevenue) as TotalBudgetRevenue
     , sum(GGG.UpdBudgetValue) as TotalBudgetValue from (
	 select distinct max(gg.gamedate) as GameDate, G.gamepk,
	 G.UpdBudgetRevenue, 
	 G.UpdBudgetValue 
	 from [FACT].[GameBudget] G
	 left join [DIM].[Schedule] GG
	 on G.gamepk = GG.gamepk
	 where GG.AwayTeamAbbr <> 'BOS'
	 group by g.gamepk, g.UpdBudgetRevenue, g.UpdBudgetValue
	 ) GGG
     group by YEAR(GameDate)) YY
          on YY.[Year] = S.[YEAR]
	 where S.AwayTeamAbbr <> 'BOS'
) ZZ
on ZZ.EventCd = E.EventCd

LEFT JOIN [BRSEDWStaging].[STG].[Stadium_Mapping_Data] MA
          ON MA.Section = S.SeatSectionCd
          AND MA.[Row] = CASE WHEN S.SeatRow BETWEEN '01' AND '09'
              THEN SUBSTRING(S.SeatRow,2,1) ELSE S.SeatRow END
		  AND MA.[Seat] = S.SeatNum

CROSS JOIN BRSEDWStaging.[STG].[Report_Run_Date] RRD

group by 
AC.AccountId,
AC.AccountName,
OP.OperatorFormattedName,
T.TicketId,
BT.BuyerTypeGroupCd,
BT.BuyerTypeGroupDesc,
BT.BuyerTypeCd,
BT.BuyerTypeDesc,
E.EventCd,
ZZ.EventDate,
E.EventDate,
EE.EventDate,
E.EventDesc,
EE.EventCd,
D.FullDate,
PS.PriceScaleCd,
P.PackageCd,
PL.PackageLineOrderOriginCd,
DL.DeliveryMethodDesc,
DL.DeliveryMethodCd,
S.SeatNum,
S.SeatRow,
S.SeatSectionCd,
A.AgencyCd,
A.AgencyDesc,
ZZ.AwayTeamAbbr,
ZZ.AwayTeam,
ZZ.Comment,
ZZ.TierNumber,
ZZ.HomeStandNumber,
ZZ.GameTimeLocal,
ZZ.GameDate,
ZZ.VenueName,
POL.PatronOrderLineTransactionTypeCd,
RRD.Tableau_Run_Date,
MA.CenterX,
MA.CenterY