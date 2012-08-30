SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

IF  NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[WorkItems]') AND type in (N'U'))

CREATE TABLE [dbo].[WorkItems](
	[WorkItemID] [int] IDENTITY(1,1) NOT NULL,
	[BindingKey] [nvarchar](1000) NOT NULL,
	[InnerMessage] [varbinary](max) NOT NULL,
	[TextData] [nvarchar](max) NULL,
 CONSTRAINT [PK_WorkItems] PRIMARY KEY CLUSTERED 
(
	[WorkItemId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

IF  NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[WorkItemStatus]') AND type in (N'U'))

CREATE TABLE [dbo].[WorkItemStatus](
	[WorkItemID] [int] NOT NULL,
	[Status] [tinyint] NULL,
	[WakeTime] [datetime] NULL,
	[ClientID] [tinyint] NULL,
	[PurgeDate] [smalldatetime] NULL,
 CONSTRAINT [PK_workItemStatus] PRIMARY KEY CLUSTERED 
(
	[WorkItemID] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

/****** Object:  Index [IX_workStatus_purgeDate]    Script Date: 11/25/2011 15:44:52 ******/

IF  NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dbo].[WorkItemStatus]') AND name = N'IX_workItemStatus_purgeDate')

CREATE NONCLUSTERED INDEX [IX_workItemStatus_purgeDate] ON [dbo].[WorkItemStatus] 
(
	[PurgeDate] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
GO

/****** Object:  Index [IX_workStatus_status_wakeTime]    Script Date: 11/25/2011 15:44:52 ******/
IF  NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dbo].[WorkItemStatus]') AND name = N'IX_workItemStatus_status_wakeTime')

CREATE NONCLUSTERED INDEX [IX_workItemStatus_status_wakeTime] ON [dbo].[WorkItemStatus] 
(
	[Status] ASC,
	[WakeTime] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
GO

IF  NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_WorkItemStatus_WorkItems]') AND parent_object_id = OBJECT_ID(N'[dbo].[WorkItemStatus]'))

ALTER TABLE [dbo].[WorkItemStatus]  WITH CHECK ADD  CONSTRAINT [FK_WorkItemStatus_WorkItems] FOREIGN KEY([WorkItemID])
REFERENCES [dbo].[WorkItems] ([WorkItemID])
ON DELETE CASCADE
GO
IF  NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_WorkItemStatus_WorkItems]') AND parent_object_id = OBJECT_ID(N'[dbo].[WorkItemStatus]'))

ALTER TABLE [dbo].[WorkItemStatus] CHECK CONSTRAINT [FK_WorkItemStatus_WorkItems]
GO

/****** Object:  StoredProcedure [dbo].[usp_addNewMessageToScheduler]    Script Date: 11/25/2011 15:41:52 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[uspAddNewMessageToScheduler]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[uspAddNewMessageToScheduler]
GO

CREATE PROCEDURE [dbo].[uspAddNewMessageToScheduler] 
	@WakeTime DATETIME,
	@BindingKey NVARCHAR(1000),
	@Message VARBINARY(MAX)
AS
/******************************************************************************
**		File: uspAddNewMessageToScheduler.sql
**		Name: uspAaddNewMessageToScheduler 
**		Desc: Dummy update script, to test concurrency on workItems table
**
**
**		Auth: Steve Smith
**		Date: 20111115
**
*******************************************************************************
**		Change History
*******************************************************************************
**		Date:		Author:				Description:
**		--------	--------			-------------------------------------------
**		20111115	Steve Smith			Original creation for demonstration
*******************************************************************************/

DECLARE @NewID INT

BEGIN TRANSACTION

INSERT INTO WorkItems (BindingKey, InnerMessage)
VALUES (@BindingKey,@Message )
-- get the ID of the inserted record for use in the child table
SELECT @NewID = SCOPE_IDENTITY()
IF @@ERROR > 0
	ROLLBACK TRANSACTION
ELSE
	-- only setup the child status record if the WorkItem insert succeeded
	BEGIN
		INSERT INTO WorkItemStatus (WorkItemID, [Status], WakeTime)
		OUTPUT INSERTED.WorkItemID, INSERTED.status, INSERTED.WakeTime
		VALUES (@NewID, 0, @WakeTime)
    	
		IF @@ERROR > 0 
			ROLLBACK TRANSACTION
		ELSE
			BEGIN
				 COMMIT TRANSACTION
			END 
	END 
--WAITFOR DELAY '00:00.005'  -- delay for use in throttling during testing

GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[uspGetNextBatchOfMessages]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[uspGetNextBatchOfMessages]
GO


/****** Object:  StoredProcedure [dbo].[USP_GetNextBatchOfMessages]    Script Date: 11/25/2011 15:08:14 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[uspGetNextBatchOfMessages]
 @rows INT = 1000, 
 @status TINYINT = 0,
 @WakeTime DATETIME

AS
/******************************************************************************
**		File: uspGetNextBatchOfMessages.sql
**		Name: uspGetNextBatchOfMessages 
**		Desc: Example table polling technique for scheduled tasks
**
**		Auth: Steve Smith
**		Date: 20111115
**
**      Uses: @rows = number of rows to select for update
**			  @status = status to seek 
**			  @waketime = seek date/time EARLIER than this
*******************************************************************************
**		Change History
*******************************************************************************
**		Date:		Author:				Description:
**		--------	--------			-------------------------------------------
**		20111115	Steve Smith			Original creation for demonstration
*******************************************************************************/


-- NB: WITH statements require a ';' on the statement immediately previous
BEGIN TRANSACTION;


-- Uses a CTE to allow ORDER BY WakeTime, and to throttle by @rows
-- (because you cannot ORDER BY an UPDATE statement)
WITH Results as
(
SELECT TOP (@rows) WorkItemID, WakeTime 
FROM WorkItemStatus ws
WHERE ws.Status = @status and ws.Waketime <= @WakeTime
ORDER BY ws.WakeTime ASC
)
-- Performs the UPDATE and OUTPUTs the INSERTED. fields to the calling app
UPDATE WorkItemStatus
SET Status = 2
OUTPUT INSERTED.WorkItemID, 2 as Status, INSERTED.WakeTime, wi.BindingKey, wi.InnerMessage
FROM WorkItemStatus ws
INNER JOIN Results r       -- this JOIN filters our UPDATE to the @rows SELECTed
ON r.WorkItemID = ws.WorkItemID
INNER JOIN WorkItems wi    -- this JOIN is purely to allow OUTPUT of Bindingkey and InnerMessage
ON ws.WorkItemID = wi.WorkItemID

IF @@ERROR > 0 
	ROLLBACK TRANSACTION
ELSE
	COMMIT TRANSACTION
	
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[uspMarkWorkItemForPurge]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[uspMarkWorkItemForPurge]
GO

/****** Object:  StoredProcedure [dbo].[uspGetNextBatchOfMessages]    Script Date: 11/25/2011 16:16:13 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[uspMarkWorkItemForPurge]
 @ID INT = 0, @purgeDate datetime = NULL

AS
/******************************************************************************
**		File: uspMarkWorkItemForPurge.sql
**		Name: uspMarkWorkItemForPurge 
**		Desc: Example table purgeing technique
**
**		Auth: Steve Smith
**		Date: 20111125
**
**      Uses: @ID = record to update
**			  @purgeDate = date to purge record 
*******************************************************************************
**		Change History
*******************************************************************************
**		Date:		Author:				Description:
**		--------	--------			-------------------------------------------
**		20111125	Steve Smith			Original creation for demonstration
*******************************************************************************/
-- Set default purgeDate to Now
IF @purgeDate is NULL SET @purgeDate=getdate()

-- Performs the UPDATE and OUTPUTs the INSERTED. fields to the calling app
UPDATE WorkItemStatus
SET PurgeDate = @purgeDate
OUTPUT INSERTED.WorkItemID, INSERTED.purgeDate
FROM WorkItemStatus ws
WHERE WorkItemID = @ID

GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[uspWorkItemsSelfPurge]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[uspWorkItemsSelfPurge]
GO

/****** Object:  StoredProcedure [dbo].[uspWorkItemsSelfPurge]    Script Date: 11/25/2011 15:05:49 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE Procedure [dbo].[uspWorkItemsSelfPurge] @rows TinyINT = 5, @purgeDate DateTime = NULL 

AS
/******************************************************************************
**		File: uspWorkItemsSelfPurge.sql
**		Name: uspWorkItemsSelfPurge 
**		Desc: Example table purging technique to run as a regular scheduled task
**
**		Auth: Steve Smith
**		Date: 20111115
**
**      Uses: @rows = number of rows to delete at a time
**				@purgeDate = date to delete, defaults to now
*******************************************************************************
**		Change History
*******************************************************************************
**		Date:		Author:				Description:
**		--------	--------			-------------------------------------------
**		20111125	Steve Smith			Original creation for demonstration
*******************************************************************************/

IF @purgeDate is NULL SET @purgeDate=getdate()

-- Only execute if there is work to do and continue 
-- until all records with a PurgeDate <= now are deleted
WHILE EXISTS(SELECT * FROM WorkItemStatus WHERE PurgeDate <= @purgeDate) 
BEGIN
	-- NB:  the FK in WorkStatus has ON DELETE CASCADE,
	-- so it will delete corresponding rows automatically
	DELETE TOP (@rows) 
	FROM WorkItemStatus
	WHERE PurgeDate <= @purgeDate
END -- WHILE EXISTS(

GO