use hrms40
go

if exists ( select 'y' from sysobjects where name = 'byjus_ars_ssis_sp' and type = 'P')
    drop proc byjus_ars_ssis_sp
go



 

create proc byjus_ars_ssis_sp
as
begin
set nocount on

declare @sysdate			date = getdate()
		,@first_date		date
		,@guid				udd_guid = newid()
		,@cur_attend_date	date
begin try

--remove the below update after testing 
		update byjus_ars_ssis_tbl
		set    employee_code = employee_code 
			   ,attendance_date	= cast(attendance_date as date)
		
		select @cur_attend_date = attendance_date
		from   byjus_ars_ssis_tbl

		delete byjus_ars_ssis_tbl where attendance_date <> @cur_attend_date 

		select @first_date = DATEADD(month, DATEDIFF(month, 0, min(attendance_date)), 0) 
		from   byjus_ars_ssis_tbl
		
		declare @temp table
		(
		master_ou				int
		,employee_code			nvarchar(100)
		,attendance_date		date
		,attendance_in_time		datetime
		,attendance_out_time	datetime
		,guid1					nvarchar(800)
		)

		create table #source
		(
		master_ou				int
		,employee_code			nvarchar(100)
		,attendance_date		date
		,attendance_in_time		datetime
		,attendance_out_time	datetime
		,shift_code				nvarchar(30)
		,shift_start_date_time	datetime
		,shift_end_date_time	datetime
		,shift_total_time		int
		,regular_hours			int
		,late_coming_hours		int
		,early_going_hours		int
		,ot_hours				numeric(15,4)
		,guid					nvarchar(800)
		,exception_status		nvarchar(10)
		,shift_tolerance_limit  int
		)
		
		
		insert @temp (master_ou
					,employee_code
					,attendance_date
					,guid1
					)
		select	asgn.master_ou_code
				,a.employee_code
				,a.attendance_date
				,newid()
		from    byjus_ars_ssis_tbl a 
		join	hrei_asgn_eff_auth_dtl (nolock) asgn
				on     a.employee_code = asgn.employee_code
				and    asgn.assignment_auth_status = 'A'
				and    a.attendance_date between assignment_effective_from_date	and isnull(asgn.assignment_effective_to_date,a.attendance_date)
		group by asgn.master_ou_code
				,a.employee_code
				,a.attendance_date
		
		;with attendance_in_cte as
		(
		select  master_ou_code
				,employee_code
				,attendance_date
				,min(attendance_in_time) 'attendance_in_time'
		from 
		(
		select	asgn.master_ou_code
				,a.employee_code
				,a.attendance_date
				,cast(concat(a.attendance_date,space(1),a.attendance_time) as datetime)   attendance_in_time
		from    byjus_ars_ssis_tbl a 
		join	hrei_asgn_eff_auth_dtl (nolock) asgn
				on     a.employee_code = asgn.employee_code
				and    asgn.assignment_auth_status = 'A'
				and    a.attendance_date between assignment_effective_from_date	and isnull(asgn.assignment_effective_to_date,a.attendance_date)
		where   a.io_flag = 'i'
		union all
		select t.master_ou,t.employee_code,t.tmsht_date,tmsht_from_date
						
		from   tmsht_hourly_based_dtl t (nolock)
		join   byjus_ars_ssis_tbl s 
				on t.employee_code = s.employee_code
				and t.tmsht_date = s.attendance_date

		
		) a
		group by a.master_ou_code
				,a.employee_code
				,a.attendance_date
		)

		update a
		set    attendance_in_time = b.attendance_in_time
		from   @temp a 
		join   attendance_in_cte b
				on  a.master_ou		  = b.master_ou_code
				and a.employee_code	  = b.employee_code
				and a.attendance_date = b.attendance_date
		
		;with attendance_out_cte as
		(
		select  master_ou_code
				,employee_code
				,attendance_date
				,max(attendance_out_time) 'attendance_out_time'
		from 
		(
		select	asgn.master_ou_code
				,a.employee_code
				,a.attendance_date
				,cast(concat(a.attendance_date,space(1),a.attendance_time) as datetime)   attendance_out_time
		from    byjus_ars_ssis_tbl a 
		join	hrei_asgn_eff_auth_dtl (nolock) asgn
				on     a.employee_code = asgn.employee_code
				and    asgn.assignment_auth_status = 'A'
				and    a.attendance_date between assignment_effective_from_date	and isnull(asgn.assignment_effective_to_date,a.attendance_date)
		where   a.io_flag = 'o'
		union all
		select t.master_ou,t.employee_code,t.tmsht_date,tmsht_from_date
						
		from   tmsht_hourly_based_dtl t (nolock)
		join   byjus_ars_ssis_tbl s 
				on t.employee_code = s.employee_code
				and t.tmsht_date = s.attendance_date

		
		) a
		group by a.master_ou_code
				,a.employee_code
				,a.attendance_date
		)
		
		update a
		set    attendance_out_time = b.attendance_out_time
		from   @temp a 
		join   attendance_out_cte b
				on  a.master_ou		  = b.master_ou_code
				and a.employee_code	  = b.employee_code
				and a.attendance_date = b.attendance_date


		;with attendance_max_in_cte as
		(
		select  master_ou_code
				,employee_code
				,attendance_date
				,max(attendance_in_time) 'attendance_in_time'
		from 
		(
		select	asgn.master_ou_code
				,a.employee_code
				,a.attendance_date
				,cast(concat(a.attendance_date,space(1),a.attendance_time) as datetime)   attendance_in_time
		from    byjus_ars_ssis_tbl a 
		join	hrei_asgn_eff_auth_dtl (nolock) asgn
				on     a.employee_code = asgn.employee_code
				and    asgn.assignment_auth_status = 'A'
				and    a.attendance_date between assignment_effective_from_date	and isnull(asgn.assignment_effective_to_date,a.attendance_date)
		where   a.io_flag = 'i'
		union all
		select t.master_ou,t.employee_code,t.tmsht_date,tmsht_from_date
						
		from   tmsht_hourly_based_dtl t (nolock)
		join   byjus_ars_ssis_tbl s 
				on t.employee_code = s.employee_code
				and t.tmsht_date = s.attendance_date

		
		) a
		group by a.master_ou_code
				,a.employee_code
				,a.attendance_date
		)

		update a
		set    attendance_out_time = b.attendance_in_time
		from   @temp a 
		join   attendance_max_in_cte b
				on  a.master_ou		  = b.master_ou_code
				and a.employee_code	  = b.employee_code
				and a.attendance_date = b.attendance_date
				and b.attendance_in_time > a.attendance_out_time 

		if exists ( select '%' 
					from   tmsht_hourly_based_dtl (nolock) 
					where  tmsht_date = @first_date 
					)
		begin
				select ''
		end
		else
		begin	

				with daysinmonth as 
				(
					select @first_date as date
					union all
					select dateadd(dd,1,date)
					from daysinmonth
					where month(date) = month(@first_date)
				)

				insert #source (guid,master_ou,employee_code,attendance_date)
				select  
				newid()
				,asgn.master_ou_code
			   ,asgn.employee_code
			   ,date
				from   hrei_asgn_eff_auth_dtl (nolock) asgn
				       ,daysinmonth 
				where  month(date) = month(@first_date)
				and    asgn.assignment_auth_status = 'A'
				and    asgn.assignment_effective_to_date is null

		
		end 

		merge into #source as t
			using	@temp	as	s
					on		t.employee_code = s.employee_code
					and		t.attendance_date	  = s.attendance_date
			when	matched	then
					update	set	 t.attendance_in_time		=	s.attendance_in_time
								,t.attendance_out_time		=	s.attendance_out_time
								,t.regular_hours			=	(datediff(ss,s.attendance_in_time,isnull(s.attendance_out_time,s.attendance_in_time)) / 3600.00) * 60
			when	not	matched	then
					insert
					(guid ,master_ou,employee_code        , attendance_date          , attendance_in_time			    , attendance_out_time    , regular_hours)
					values
					(
					s.guid1,s.master_ou,s.employee_code			   ,s.attendance_date					,s.attendance_in_time				,s.attendance_out_time
					,(datediff(ss,s.attendance_in_time,isnull(s.attendance_out_time,s.attendance_in_time)) / 3600.00) * 60)
					;
		---Update the shift code for each employees

		update	tmp
		set		shift_code = t.shift_code
		from	#source	tmp
		join	tmscd_emp_gre_calendar t
				on	tmp.master_ou	=	t.master_ou
				and tmp.employee_code	=	t.employee_code
				and	tmp.attendance_date	=	t.schedule_date
		
		
--Replace shift code into deviation shift code if available for particular employee

		update	tmp
		set		shift_code	=	deviate_shift_to
		from	#source	tmp
		join	tmscd_emp_gre_shift_devn_vw	d
				on	tmp.master_ou	=	d.master_ou
				and	tmp.employee_code	=	d.employee_code
				and	tmp.attendance_date	between	d.eff_from_date	and	d.eff_to_date

--Find shift total hours
		
		update	tmp
		set		shift_total_time = (datediff(ss,shift_start_time,shift_end_time) / 3600.00) * 60
				,shift_start_date_time = concat(attendance_date,space(1),shift_start_time)
				,shift_end_date_time = concat(attendance_date,space(1),shift_end_time)
				,shift_tolerance_limit = s.shift_tolerance_limit
		from	#source tmp
		join	tmgif_shift_vw s
				on	tmp.shift_code	=	s.shift_code
				and	s.language_code	=	1
				and	tmp.master_ou	=	s.master_ou

--Find OT hours			  
		
		update	#source
		set		ot_hours	=	regular_hours	-	shift_total_time
		where	regular_hours	>	shift_total_time


--Find late coming hours

		update	#source
		set		late_coming_hours = (datediff(ss,shift_start_date_time,attendance_in_time) / 3600.00) * 60
		where	attendance_in_time	>	dateadd(minute,shift_tolerance_limit,shift_start_date_time)
				 
--Find Early going hours

		update	#source
		set		early_going_hours	=	(datediff(ss,attendance_out_time,shift_end_date_time) / 3600.00) * 60
		where	shift_end_date_time	<	dateadd(minute,-shift_tolerance_limit,attendance_out_time) 

--Update MissedIn and Missed Out 
--Find Missed in
		update #source
		set	   exception_status = 'EXMI'
		where  attendance_in_time is null

--Find Missed Out
		update #source
		set	   exception_status = 'EXMO'
		where  attendance_out_time is null

		update #source
		set	   exception_status = 'EXMO'
		where  attendance_out_time = attendance_in_time

 
		update #source
		set	   exception_status = 'EXAB'
		where  attendance_in_time  is null
		and    attendance_out_time is null
		and    shift_code <> 'OFF'
--If attendance date is holiday then regular hours is 0 and ot hours should be regular hours
		update s
		set    ot_hours = regular_hours
				,regular_hours = 0.00
		from   #source s
		join   tmgif_holiday_master_dtl d (nolock)
			   on s.attendance_date  = d.holiday_date

--For Other than Missed in and Missed out
		update #source
		set	   exception_status = 'EXOK' 
		where  exception_status is null


--Populate all the values into timesheet tables 
		
 
		merge into dbo.tmsht_hourly_based_dtl as t
		using	#source	as	s
				on		t.master_ou			= s.master_ou
				and		t.employee_code		= s.employee_code
				and		t.tmsht_date		= s.attendance_date
		when	matched	then
				update	set	 tmsht_from_date    = isnull(s.attendance_in_time,t.tmsht_from_date)
							,rounded_from_date  = isnull(s.attendance_in_time,t.rounded_from_date)
							,clock_in_date		= isnull(s.attendance_in_time,t.clock_in_date)
							,t.tmsht_to_date    = isnull(s.attendance_out_time,t.tmsht_to_date)
							,t.rounded_to_date  = isnull(s.attendance_out_time,t.rounded_to_date)
							,t.clock_out_date   = isnull(s.attendance_out_time,t.clock_out_date)
							,t.regular_hours    = isnull(s.regular_hours,t.regular_hours)
							,t.ot_hours			= isnull(s.ot_hours,t.ot_hours)
							,t.late_hours		= isnull(s.late_coming_hours,t.late_hours)
							,t.early_hours		= isnull(s.early_going_hours,t.early_hours)
							,t.shift			= isnull(s.shift_code,t.shift)
							,t.exception_status = isnull(s.exception_status,t.exception_status)
		when	not	matched	then
				insert
				(prim_guid        , timestamp          , guid			    , employee_code    , assignment_no    , tmsht_date  
				,tmsht_from_date  , rounded_from_date  , tmsht_to_date      , rounded_to_date  , exception_status , timesheet_status   
				, master_ou	      , empng_ou		   ,empin_ou  ,tmprc_ou			, tmsch_ou         , createdby		  , createddate	 
				, clock_in_date   , clock_out_date	   ,regular_hours		, ot_hours		   , late_hours		  ,early_hours , shift
				)
				values
				(
				s.guid			   ,0					,@guid				,s.employee_code		,1				,s.attendance_date
				,s.attendance_in_time, s.attendance_in_time  ,s.attendance_out_time,s.attendance_out_time ,  exception_status		,'AUTH'/*'PA'*/
				,s.master_ou		,s.master_ou			,s.master_ou			,s.master_ou				,s.master_ou			,'BE'				,@sysdate
				,s.attendance_in_time ,s.attendance_out_time ,s.regular_hours ,s.ot_hours			,s.late_coming_hours ,s.early_going_hours , s.shift_code
				);
 
-- 		truncate table byjus_ars_ssis_tbl

end try
begin catch
 
		insert byjus_ars_processed_status
		(
			empcode			
			,attendance_date
			,attendance_time
			,io_flag		
			,file_name1								
			,date
			,status
			,err_id
			,err_desc
		)
		select 
			employee_code			
			,attendance_date
			,attendance_time
			,io_flag		
			,file_name1								
			,@sysdate
			,'F'
			,error_number()
			,error_message()
		from byjus_ars_ssis_tbl

		truncate table byjus_ars_ssis_tbl
end catch
 
set nocount off
end 





go

if exists ( select 'y' from sysobjects where name = 'byjus_ars_ssis_sp' and type = 'P')
    grant exec on byjus_ars_ssis_sp to public
go
