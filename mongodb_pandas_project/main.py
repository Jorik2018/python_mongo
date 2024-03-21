import argparse

def filter_collection(collection):
    if collection == 'user':
        from mongodb_pandas_project.filters.user_filter import filter
        filter()
    elif collection == 'company':
        from mongodb_pandas_project.filters.company_filter import filter
        filter()
    elif collection == 'case':
        from mongodb_pandas_project.filters.case_filter import filter
        filter()
    else:
        print('No valid collection to filter')

def main():
    parser = argparse.ArgumentParser(description='Export/Import MongoDB data to/from CSV file.')
    parser.add_argument('operation', choices=['export', 'restore','collections','dump','csv','filter','start','test'], help='Operation to perform: export or import')
    parser.add_argument('--collection', required=False, help='Collection name')
    parser.add_argument('--fields', required=False, help='Fields from collection to export in csv columns')
    parser.add_argument('--backup', required=False, help='Folder name of backup')
    parser.add_argument('--db', required=False, help='Database to restore backup')

    args = parser.parse_args()
    if args.operation == 'start':
        from mongodb_pandas_project.backup_script import generate_backup
        generate_backup('user,case')
        filter_collection('user')
        filter_collection('case')
    elif args.operation == 'dump':
        from mongodb_pandas_project.backup_script import generate_backup
        generate_backup(args.collection)
    elif args.operation == 'restore':
        from mongodb_pandas_project.backup_script import restore_backup
        restore_backup(args.backup,args.db)
    elif args.operation == 'filter':
        filter_collection(args.collection)
    elif args.operation == 'test':
        from mongodb_pandas_project.backup_script import test
        test()

if __name__ == '__main__':
    main()
