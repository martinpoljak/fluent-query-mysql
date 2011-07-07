# encoding: utf-8
require "fluent-query/drivers/dbi"
require "fluent-query/drivers/exception"
require "fluent-query/drivers/shared/tokens/sql"

module FluentQuery
    module Drivers

         ##
         # MySQL database driver.
         #
         
         class MySQL < FluentQuery::Drivers::DBI

            ##
            # Contains relevant methods index for this driver.
            #
                
            RELEVANT = [:select, :insert, :update, :delete, :truncate, :set, :begin, :commit, :union]

            ##
            # Contains ordering for typicall queries.
            #
            
            ORDERING = {
                :select => [
                    :select, :from, :join, :groupBy, :having, :where, :orderBy, :limit, :offset
                ],
                :insert => [
                    :insert, :values
                ],
                :update => [
                    :update, :set, :where
                ],
                :delete => [
                    :delete, :where
                ],
                :truncate => [
                    :truncate, :table, :cascade
                ],
                :set => [
                    :set
                ],
                :union => [
                    :union
                ]
            }

            ##
            # Contains operators list.
            #
            # Operators are defined as tokens whose multiple parameters in Array
            # are appropriate to join by itself.
            #
            
            OPERATORS = {
                :and => "AND",
                :or => "OR"
            }

            ##
            # Indicates, appropriate token should be present by one real token, but more input tokens.
            #

            AGREGATE = [:where, :orderBy, :select]

            ##
            # Indicates token aliases.
            #

            ALIASES = {
                :leftJoin => :join,
                :rightJoin => :join,
                :fullJoin => :join
            }

            ##
            # Indicates tokens already required.
            #
            
            protected
            @_tokens_required

            ##
            # Known tokens index.
            # (internal cache)
            #

            @@__known_tokens = Hash::new do |hash, key| 
                hash[key] = { }
            end

            ##
            # Initializes driver.
            #

            public
            def initialize(connection)
                super(connection)

                @relevant = self.class::RELEVANT
                @ordering = self.class::ORDERING
                @operators = self.class::OPERATORS
                @aliases = self.class::ALIASES

                self.class::AGREGATE.each do |i| 
                    @agregate[i] = true
                end
                
                @_tokens_required = { }
            end

            ##
            # Indicates token is known.
            #

            public
            def known_token?(group, token_name)
                super(group, token_name, @@__known_tokens)
            end


            ##### QUOTING

            ##
            # Quotes string.
            #

            public
            def quote_string(string)
                "E" << super(string)
            end
            
            ##
            # Quotes system-dependent boolean value.
            #

            public
            def quote_boolean(boolean)
                boolean ? "TRUE" : "FALSE"
            end


            ##### EXECUTING
            

            ##
            # Builds connection string.
            # @return [String] connection string
            #
            
            public
            def connection_string
                if @_nconnection_settings.nil?
                    raise FluentQuery::Drivers::Exception::new('Connection settings hasn\'t been assigned yet.')
                end
                
                # Gets settings
                
                server = @_nconnection_settings[:server]
                port = @_nconnection_settings[:port]
                socket = @_nconnection_settings[:socket]
                encoding = @_nconnection_settings[:encoding]
                schema = @_nconnection_settings[:schema]
                database = @_nconnection_settings[:database]
                username = @_nconnection_settings[:username]
                password = @_nconnection_settings[:password]
                
                # Builds connection string and other parameters
                
                if server.nil?
                    server = "localhost"
                end
                
                connection_string = "DBI:Pg:database=%s;host=%s" % [database, server]
                
                if not port.nil?
                    connection_string << ";port=" << port.to_s
                end
                if not socket.nil?
                    connection_string << ";socket=" << socket
                end

                # Returns 
                return connection_string
                
            end

            ##
            # Opens the connection.
            #
            # It's lazy, so it will open connection before first request through
            # {@link native_connection()} method.
            #

            public
            def open_connection(settings)
                if not settings[:database] or not settings[:username]
                    raise FluentQuery::Drivers::Exception::new("Database name and username is required for connection.")
                end
                
                super(settings)
            end

            ##
            # Returns native connection.
            #

            public
            def native_connection
            
                super()

                # Gets settings
                encoding = @_nconnection_settings[:encoding]
                
                if encoding.nil?
                    encoding = "UTF8"
                end

                # Sets encoding and default schema
                @_nconnection.do("SET NAMES " << self.quote_string(encoding) << ";")

                if not schema.nil?
                    @_nconnection.do("SET search_path = " << schema << ", pg_catalog;")
                end

                return @_nconnection
                
            end

            ##
            # Executes query conditionally.
            #
            # If query isn't suitable for executing, returns it. In otherwise
            # returns result or number of changed rows.
            #

            public
            def execute_conditionally(query, sym, *args, &block)
                case query.type
                    when :insert
                        if (args[0].kind_of? Symbol) and (args[1].kind_of? Hash)
                            result = query.do!
                        end
                    when :begin
                        if args.empty?
                            result = query.execute!
                        end
                    when :truncate
                        if args.first.kind_of? Symbol
                            result = query.execute!
                        end
                    when :commit, :rollback
                        result = query.execute!
                    else
                        result = nil
                end
                
                return result
            end
        end
    end
end

