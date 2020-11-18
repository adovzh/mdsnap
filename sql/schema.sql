-- wipe out database
drop table if exists t_snap_source cascade;
drop table if exists t_security cascade;
drop table if exists t_job_status cascade;
drop table if exists t_job cascade;
drop table if exists t_quote cascade;
drop table if exists t_portfolio cascade;
drop table if exists t_trade cascade;

drop function if exists job_insert();
drop function if exists start_job(p_snap_source varchar);
drop procedure if exists complete_job(p_job_id integer, p_job_status varchar);
drop procedure if exists trade_import(p_date_text varchar, p_portfolio_text varchar, p_security_text varchar, p_buy_flag_text varchar, p_units numeric, p_amount numeric);
drop procedure if exists create_security(p_security_name varchar, p_security_description varchar, p_snap_source varchar);

-- Market data source
create table t_snap_source
(
    snap_source_id serial constraint pk_snap_source primary key,
    snap_source_name varchar not null unique,
    snap_source_description varchar
);

comment on table t_snap_source is 'Market data source';

-- represents a security, such as stock, etf or cash/loan
create table t_security
(
    security_id serial constraint pk_security primary key,
    security_name varchar not null unique,
    security_description varchar,
    snap_source_id integer references t_snap_source (snap_source_id)
);

comment on table t_security is 'Security such as stock or etf';

create procedure create_security(p_security_name varchar, p_security_description varchar, p_snap_source varchar default null) as
$$
declare
    v_snap_source_id integer := null;
begin
    if p_snap_source is not null then
        select snap_source_id into v_snap_source_id from t_snap_source where snap_source_name = p_snap_source;
        if not FOUND then
            raise exception 'Snap source % not found', p_snap_source;
        end if;
    end if;

    insert into t_security (security_name, security_description, snap_source_id)
        values (p_security_name, p_security_description, v_snap_source_id);
    raise notice 'Security % (%) is created', p_security_name, p_security_description;
end;
$$ language plpgsql;

-- the status of a job (started/completed/failed)
create table t_job_status
(
	job_status_id serial not null constraint t_job_status_pk primary key,
	job_status_name varchar(20) not null
);


-- market data snapping job
create sequence seq_job;

create table t_job
(
	job_id int not null default nextval('seq_job') constraint t_job_pk primary key,
	job_status_id int not null references t_job_status (job_status_id),
	snap_status_id integer references t_snap_source (snap_source_id),
	created_on timestamp not null,
	modified_on timestamp
);

alter sequence seq_job owned by t_job.job_id;
create index idx_job_created_on  on t_job (created_on);

create function job_insert() returns trigger as
$$
begin
    if new.job_status_id is null then
        select job_status_id into new.job_status_id from t_job_status where job_status_name='STARTED';
    end if;
    if new.created_on is null then
        new.created_on := current_timestamp;
    end if;
    return new;
end;
$$ language plpgsql;

create trigger job_insert before insert on t_job
    for each row execute procedure job_insert();

create function start_job(p_snap_source varchar) returns integer as
$$
declare
    v_job_id integer;
    v_job_status_id integer;
    v_snap_source_id integer;
begin
    select nextval('seq_job') into v_job_id;
    select job_status_id into v_job_status_id from t_job_status where job_status_name = 'STARTED';

    if not FOUND then
        raise exception 'Corrupted metadata: job status STARTED not found';
    end if;

    select snap_source_id into v_snap_source_id from t_snap_source
        where snap_source_name = p_snap_source;

    if not FOUND then
        raise exception 'Snap source % not found', p_snap_source;
    end if;

    insert into t_job (job_id, job_status_id, snap_status_id, created_on)
        values (v_job_id, v_job_status_id, v_snap_source_id, current_timestamp);

    return v_job_id;
end;
$$ language plpgsql;

create procedure complete_job(p_job_id integer, p_job_status varchar) as
$$
declare
    v_job_status_id integer;
begin
    perform job_id from t_job j
        inner join t_job_status s on j.job_status_id = s.job_status_id
        where job_id=p_job_id and s.job_status_name = 'STARTED';
    if not FOUND then
        raise exception 'Job % does not exist or completed', p_job_id;
    end if;

    select job_status_id into v_job_status_id from t_job_status where job_status_name = p_job_status;
    if not FOUND then
        raise exception 'Corrupted metadata: job status % not found', p_job_status;
    end if;

    update t_job set job_status_id = v_job_status_id, modified_on = current_timestamp
        where job_id = p_job_id;
end
$$ language plpgsql;

-- market quote ohlc
create table t_quote
(
	security_id int not null references t_security (security_id),
	job_id int not null references t_job (job_id),
	quote_date date not null,
	quote_open numeric(10,2),
	quote_high numeric(10,2),
	quote_low numeric(10,2),
	quote_close numeric(10,2),
	quote_volume integer,
	quote_adjusted numeric(12,4),
	unique (security_id, job_id, quote_date)
);

comment on table t_quote is 'Market Quotes';

create index idx_quote_security_id_job_ids
	on t_quote (security_id, job_id);

-- portfolio of securities
create table t_portfolio
(
    portfolio_id serial constraint t_portfolio_pk primary key,
    portfolio_name varchar not null
);

-- trade, a transaction in a portfolio
create table t_trade
(
    trade_id serial constraint t_trade_pk primary key,
    trade_date date not null,
    portfolio_id int not null references t_portfolio (portfolio_id),
    security_id int not null references t_security (security_id),
    trade_buy_flag boolean not null,
    trade_units numeric,
    trade_amount numeric(10, 2)
);

create index idx_trade_date on t_trade (trade_date);

create procedure trade_import(p_date_text varchar, p_portfolio_text varchar,
    p_security_text varchar, p_buy_flag_text varchar, p_units numeric, p_amount numeric) as
$$
declare
    v_security_id numeric;
    v_portfolio_id numeric;
    v_buy_sell boolean;
begin
    select security_id into v_security_id from t_security where security_name = p_security_text;
    if not FOUND then
        raise exception 'Security % not found', p_security_text;
    end if;

    select portfolio_id into v_portfolio_id from t_portfolio where portfolio_name = p_portfolio_text;
    if not FOUND then
        raise exception 'Portfolio % not found', p_portfolio_text;
    end if;

    v_buy_sell := lower(p_buy_flag_text) = 'buy';
    insert into t_trade (trade_date, security_id, portfolio_id, trade_buy_flag, trade_units, trade_amount)
        values (to_date(p_date_text, 'DD/MM/YY'), v_security_id, v_portfolio_id, v_buy_sell, p_units, p_amount);
end
$$ language plpgsql;

-- metadata
insert into t_snap_source (snap_source_name, snap_source_description) values ('quantmod', 'Quantmod R package getSymbols');
insert into t_snap_source (snap_source_name, snap_source_description) values ('csv_dir', 'CSV file in a specified directory');
insert into t_snap_source (snap_source_name, snap_source_description) values ('cash', 'Cash pseudo-security');

-- job statuses
insert into t_job_status(job_status_name) values ('STARTED');
insert into t_job_status(job_status_name) values ('COMPLETED');
insert into t_job_status(job_status_name) values ('FAILED');
