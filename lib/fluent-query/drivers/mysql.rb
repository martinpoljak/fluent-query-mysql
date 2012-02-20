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
            # Known tokens index.
            # (internal cache)
            #

            @@__known_tokens = Hash::new do |hash, key| 
                hash[key] = { }
            end

            ##
            # Indicates token is known.
            #

            public
            def known_token?(group, token_name)
                super(group, token_name, @@__known_tokens)
            end


            ##### EXECUTING

            ##
            # Returns the DBI driver name.
            # @return [String] driver name
            #
            
            public
            def driver_name
                "Mysql"
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
                    encoding = "utf8"
                end

                # Sets encoding and default schema
                @_nconnection.do("SET NAMES " + self.quote_string(encoding) + ";")

                return @_nconnection
                
            end

            ##
            # Quotes field by field quoting.
            #

            public
            def quote_identifier(field)
                '`' + field.to_s.gsub(".", '`.`') + '`'
            end
        end
    end
end

