CREATE DATABASE WarsawPublicTransport
GO
USE WarsawPublicTransport
GO
CREATE TABLE [dbo].[Calendar](
	[DayType] [nvarchar](max) NOT NULL,
	[day] [date] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
CREATE TABLE [dbo].[Delays](
	[StopNo] [int] NULL,
	[VehicleNumber] [nvarchar](max) NULL,
	[Date] [date] NULL,
	[DepTime] [nvarchar](max) NULL,
	[Delay] [bigint] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
CREATE TABLE [dbo].[Stops](
	[stop_nr] [int] NULL,
	[StopName] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
CREATE TABLE [dbo].[Trams](
	[Lines] [int] NULL,
	[VehicleNumber] [nvarchar](max) NULL,
	[Brigade] [int] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
