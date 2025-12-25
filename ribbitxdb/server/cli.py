"""
RibbitXDB Server CLI
"""

import argparse
import logging
import sys

def main():
    parser = argparse.ArgumentParser(description='RibbitXDB Server')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--port', type=int, default=5432, help='Port to listen on')
    parser.add_argument('--database', default='server.rbx', help='Database file path')
    parser.add_argument('--tls-cert', help='TLS certificate file')
    parser.add_argument('--tls-key', help='TLS private key file')
    parser.add_argument('--tls-ca', help='TLS CA certificate file')
    parser.add_argument('--require-client-cert', action='store_true', 
                       help='Require client certificate')
    parser.add_argument('--log-level', default='INFO', 
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Logging level')
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Import and start server
    try:
        from ribbitxdb.server import start_server
        
        print(f"Starting RibbitXDB Server on {args.host}:{args.port}")
        if args.tls_cert:
            print(f"TLS enabled with certificate: {args.tls_cert}")
        
        start_server(
            host=args.host,
            port=args.port,
            database_path=args.database,
            tls_cert=args.tls_cert,
            tls_key=args.tls_key,
            tls_ca=args.tls_ca,
            require_client_cert=args.require_client_cert
        )
    except KeyboardInterrupt:
        print("\nServer stopped")
        sys.exit(0)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
