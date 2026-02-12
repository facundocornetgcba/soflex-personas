"""
Test script to verify Neon download and watermark functionality.

This script tests:
1. Connection to Neon PostgreSQL
2. Download of historical data from Neon
3. Watermark (max date) detection from Neon
4. Table statistics retrieval
"""

from core.db_connections import (
    download_from_neon, 
    get_max_date_from_neon,
    get_table_stats
)

def test_neon_download():
    """Test downloading data from Neon"""
    print("=" * 60)
    print("TEST 1: Download from Neon")
    print("=" * 60)
    
    try:
        df = download_from_neon('historico_limpio')
        
        if df.empty:
            print("⚠️ DataFrame vacío - tabla no existe o está vacía")
            print("   Esto es normal para primera carga")
            return None
        
        print(f"\n✅ Descarga exitosa!")
        print(f"   Registros: {len(df):,}")
        print(f"   Columnas: {len(df.columns)}")
        
        # Verificar columnas requeridas
        required_cols = ['Fecha Inicio', 'DNI_Categorizado', 'comuna_calculada']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            print(f"\n⚠️ Columnas faltantes: {missing_cols}")
        else:
            print(f"\n✅ Todas las columnas requeridas presentes")
        
        # Mostrar primeras columnas
        print(f"\nPrimeras 5 columnas: {df.columns[:5].tolist()}")
        
        return df
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        return None


def test_watermark():
    """Test watermark detection from Neon"""
    print("\n" + "=" * 60)
    print("TEST 2: Watermark Detection from Neon")
    print("=" * 60)
    
    try:
        watermark = get_max_date_from_neon('historico_limpio', 'Fecha Inicio')
        
        if watermark is None:
            print("⚠️ No se encontró watermark (tabla vacía o no existe)")
            print("   Esto es normal para primera carga")
        else:
            print(f"\n✅ Watermark detectado: {watermark}")
            print(f"   Tipo: {type(watermark)}")
        
        return watermark
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        return None


def test_stats():
    """Test table statistics retrieval"""
    print("\n" + "=" * 60)
    print("TEST 3: Table Statistics")
    print("=" * 60)
    
    try:
        stats = get_table_stats('historico_limpio')
        
        if stats is None:
            print("⚠️ No se pudieron obtener estadísticas")
        else:
            print(f"\n✅ Estadísticas obtenidas:")
            print(f"   Total registros: {stats['total_records']:,}")
            print(f"   Fecha mínima: {stats['min_fecha']}")
            print(f"   Fecha máxima: {stats['max_fecha']}")
        
        return stats
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        return None


if __name__ == '__main__':
    print("\n🧪 INICIANDO TESTS DE FUNCIONALIDAD NEON\n")
    
    # Test 1: Download
    df = test_neon_download()
    
    # Test 2: Watermark
    watermark = test_watermark()
    
    # Test 3: Stats
    stats = test_stats()
    
    # Summary
    print("\n" + "=" * 60)
    print("RESUMEN DE TESTS")
    print("=" * 60)
    
    test_results = {
        "Download desde Neon": "✅ OK" if df is not None else "⚠️ Tabla vacía/no existe",
        "Detección de Watermark": "✅ OK" if watermark is not None else "⚠️ Sin watermark",
        "Estadísticas": "✅ OK" if stats is not None else "❌ Error"
    }
    
    for test_name, result in test_results.items():
        print(f"{test_name}: {result}")
    
    print("\n✅ Tests completados!")
