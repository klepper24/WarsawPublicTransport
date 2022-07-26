USE [master]
GO
/****** Object:  Database [WarsawPublicTransport]    Script Date: 7/26/2022 10:04:28 AM ******/
CREATE DATABASE [WarsawPublicTransport]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'WarsawPublicTransport', FILENAME = N'/var/opt/mssql/data/WarsawPublicTransport.mdf' , SIZE = 73728KB , MAXSIZE = UNLIMITED, FILEGROWTH = 65536KB )
 LOG ON 
( NAME = N'WarsawPublicTransport_log', FILENAME = N'/var/opt/mssql/data/WarsawPublicTransport_log.ldf' , SIZE = 139264KB , MAXSIZE = 2048GB , FILEGROWTH = 65536KB )
GO
ALTER DATABASE [WarsawPublicTransport] SET COMPATIBILITY_LEVEL = 140
GO
IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [WarsawPublicTransport].[dbo].[sp_fulltext_database] @action = 'enable'
end
GO
ALTER DATABASE [WarsawPublicTransport] SET ANSI_NULL_DEFAULT OFF 
GO
ALTER DATABASE [WarsawPublicTransport] SET ANSI_NULLS OFF 
GO
ALTER DATABASE [WarsawPublicTransport] SET ANSI_PADDING OFF 
GO
ALTER DATABASE [WarsawPublicTransport] SET ANSI_WARNINGS OFF 
GO
ALTER DATABASE [WarsawPublicTransport] SET ARITHABORT OFF 
GO
ALTER DATABASE [WarsawPublicTransport] SET AUTO_CLOSE OFF 
GO
ALTER DATABASE [WarsawPublicTransport] SET AUTO_SHRINK OFF 
GO
ALTER DATABASE [WarsawPublicTransport] SET AUTO_UPDATE_STATISTICS ON 
GO
ALTER DATABASE [WarsawPublicTransport] SET CURSOR_CLOSE_ON_COMMIT OFF 
GO
ALTER DATABASE [WarsawPublicTransport] SET CURSOR_DEFAULT  GLOBAL 
GO
ALTER DATABASE [WarsawPublicTransport] SET CONCAT_NULL_YIELDS_NULL OFF 
GO
ALTER DATABASE [WarsawPublicTransport] SET NUMERIC_ROUNDABORT OFF 
GO
ALTER DATABASE [WarsawPublicTransport] SET QUOTED_IDENTIFIER OFF 
GO
ALTER DATABASE [WarsawPublicTransport] SET RECURSIVE_TRIGGERS OFF 
GO
ALTER DATABASE [WarsawPublicTransport] SET  DISABLE_BROKER 
GO
ALTER DATABASE [WarsawPublicTransport] SET AUTO_UPDATE_STATISTICS_ASYNC OFF 
GO
ALTER DATABASE [WarsawPublicTransport] SET DATE_CORRELATION_OPTIMIZATION OFF 
GO
ALTER DATABASE [WarsawPublicTransport] SET TRUSTWORTHY OFF 
GO
ALTER DATABASE [WarsawPublicTransport] SET ALLOW_SNAPSHOT_ISOLATION OFF 
GO
ALTER DATABASE [WarsawPublicTransport] SET PARAMETERIZATION SIMPLE 
GO
ALTER DATABASE [WarsawPublicTransport] SET READ_COMMITTED_SNAPSHOT OFF 
GO
ALTER DATABASE [WarsawPublicTransport] SET HONOR_BROKER_PRIORITY OFF 
GO
ALTER DATABASE [WarsawPublicTransport] SET RECOVERY FULL 
GO
ALTER DATABASE [WarsawPublicTransport] SET  MULTI_USER 
GO
ALTER DATABASE [WarsawPublicTransport] SET PAGE_VERIFY CHECKSUM  
GO
ALTER DATABASE [WarsawPublicTransport] SET DB_CHAINING OFF 
GO
ALTER DATABASE [WarsawPublicTransport] SET FILESTREAM( NON_TRANSACTED_ACCESS = OFF ) 
GO
ALTER DATABASE [WarsawPublicTransport] SET TARGET_RECOVERY_TIME = 60 SECONDS 
GO
ALTER DATABASE [WarsawPublicTransport] SET DELAYED_DURABILITY = DISABLED 
GO
EXEC sys.sp_db_vardecimal_storage_format N'WarsawPublicTransport', N'ON'
GO
ALTER DATABASE [WarsawPublicTransport] SET QUERY_STORE = OFF
GO
USE [WarsawPublicTransport]
GO
/****** Object:  Table [dbo].[Calendar]    Script Date: 7/26/2022 10:04:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Calendar](
	[DayType] [nvarchar](max) NOT NULL,
	[day] [date] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Delays]    Script Date: 7/26/2022 10:04:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Delays](
	[StopNo] [int] NULL,
	[VehicleNumber] [nvarchar](max) NULL,
	[Date] [date] NULL,
	[DepTime] [nvarchar](max) NULL,
	[Delay] [bigint] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Stops]    Script Date: 7/26/2022 10:04:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Stops](
	[stop_nr] [int] NULL,
	[StopName] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Trams]    Script Date: 7/26/2022 10:04:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Trams](
	[Lines] [int] NULL,
	[VehicleNumber] [nvarchar](max) NULL,
	[Brigade] [int] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
USE [master]
GO
ALTER DATABASE [WarsawPublicTransport] SET  READ_WRITE 
GO
